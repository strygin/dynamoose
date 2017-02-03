/* eslint-disable no-invalid-this,prefer-rest-params */
'use strict';

const Q = require('q');
const hooks = require('hooks');
const _ = require('lodash');

const Table = require('./Table');
const Query = require('./Query');
const Scan = require('./Scan');
const errors = require('./errors');

//const MAX_BATCH_READ_SIZE   = 100;
const MAX_BATCH_WRITE_SIZE = 25;
const debug = require('debug')('dynamoose:model');

class Document {

  constructor(newObj) {
    if (newObj) {
      Object.assign(this, newObj);
    }

    return this;
  }

  put(options, next) {
    debug('Document#put');
    return this.MODEL.put(this, options, next);
  }

  save(options, next) {
    debug('Document#save');
    return this.MODEL.save(this, options, next);
  }

  delete(options, next) {
    debug('Document#delete');
    return this.MODEL.delete(this, options, next);
  }
}

class Model {
  constructor(name, schema, options, base) {
    const obj = class {
      constructor(newObj) {
        const document = new Document(newObj);
        Object.defineProperty(document, 'MODEL', {
          enumerable: false,
          configurable: false,
          writable: true,
          value: this, // points to the Model instance
        });

        debug('applying methods');
        _.forEach(schema.methods, (method, key) => {
          document[key] = method;
        });

        debug('applying virtuals');
        _.forEach(schema.virtuals, (virtual) => {
          virtual.applyVirtuals(document);
        });

        debug('registering hooks');
        _.forEach(hooks, (hook, key) => {
          document[key] = hook;
        });
        return document;
      }
    };

    Object.setPrototypeOf(obj, Model.prototype);
    Object.defineProperty(obj, 'name', {writable: true});
    const props = {
      table: new Table(name, schema, options, base),
      name: name,
      base: base,
      schema: schema,
      options: options,
    };
    _.forEach(props, (prop, key) => {
      obj.prototype[key] = prop;
    });
    Object.assign(obj, props);

    ['create', 'get', 'update', 'delete', // CRUD ;)
     'query', 'queryOne', 'scan', 'save', 'put',
     'batchGet', 'batchPut', 'batchDelete'].forEach((method) => {
      obj.prototype[method] = obj[method] = tcWrapper(obj[method]);
    });

    debug('applying statics');
    _.forEach(schema.statics, (method, key) => {
      obj[key] = method;
    });

    debug('applying virtuals');
    _.forEach(schema.virtuals, (virtual) => {
      virtual.applyVirtuals(obj);
    });

    debug('registering hooks');
    _.forEach(hooks, (hook, key) => {
      obj[key] = hook;
    });

    debug('table initialization');
    obj.table.init((err) => {
      if (err) {
        throw err;
      }
    });

    return obj;
  }

  put(document, options, next) {
    debug('Model#put', this);
    const deferred = Q.defer();

    const model = this;
    const schema = model.schema;

    function putItem() {
      model.base.ddb().putItem(item, (err) => {
        if (err) {
          deferred.reject(err);
        } else {
          deferred.resolve(document);
        }
      });
      return deferred.promise.nodeify(next);
    }

    try {
      //noinspection Eslint
      var item = {
        TableName: model.table.name,
        Item: schema.toDynamo(document),
      };

      options = options || {};
      if (typeof options === 'function') {
        next = options;
        options = {};
      }
      if (options.overwrite === null || options.overwrite === undefined) {
        options.overwrite = true;
      }

      if (!options.overwrite) {
        item.ConditionExpression = 'attribute_not_exists(' + schema.hashKey.name + ')';
      }
      processCondition(item, options, schema);

      debug('putItem', item);

      if (model.options.waitForActive) {
        return model.table.waitForActive().then(putItem);
      }

      return putItem();
    } catch (err) {
      deferred.reject(err);
      return deferred.promise.nodeify(next);
    }
  }

  save(obj, options, next) {
    return this.put(obj, options, next);
  }

  create(obj, options, next) {
    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }
    if (options.overwrite === null || options.overwrite === undefined) {
      options.overwrite = false;
    }
    const document = new this(obj);

    return document.save(options, next);
  }

  get(key, options, next) {
    debug('Get %j', key);
    const deferred = Q.defer();

    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    if (key === null || key === undefined) {
      deferred.reject(new errors.ModelError('Key required to get item'));
      return deferred.promise.nodeify(next);
    }

    const schema = this.schema;

    const hashKeyName = schema.hashKey.name;
    if (!key[hashKeyName]) {
      const keyVal = key;
      key = {};
      key[hashKeyName] = keyVal;
    }

    if (schema.rangeKey && !key[schema.rangeKey.name]) {
      deferred.reject(new errors.ModelError('Range key required: ' + schema.rangeKey.name));
      return deferred.promise.nodeify(next);
    }

    const getReq = {
      TableName: this.table.name,
      Key: {},
    };

    getReq.Key[hashKeyName] = schema.hashKey.toDynamo(key[hashKeyName]);

    if (schema.rangeKey) {
      const rangeKeyName = schema.rangeKey.name;
      getReq.Key[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName]);
    }

    if (options.attributes) {
      getReq.AttributesToGet = options.attributes;
    }

    if (options.consistent) {
      getReq.ConsistentRead = true;
    }

    const ThisModel = this;

    function get() {
      debug('getItem', getReq);
      ThisModel.base.ddb().getItem(getReq, (err, data) => {
        if (err) {
          debug('Error returned by getItem', err);
          return deferred.reject(err);
        }

        debug('getItem response', data);
        if (!Object.keys(data).length) {
          return deferred.resolve();
        }
        debug('getItem response', data);

        const document = new ThisModel();
        schema.parseDynamo(document, data.Item);

        debug('getItem parsed document', document);

        deferred.resolve(document);
      });
    }

    if (this.options.waitForActive) {
      this.table.waitForActive().then(get);
    } else {
      get();
    }
    return deferred.promise.nodeify(next);
  }

  update(key, update, options, next) {
    debug('Update %j', key);
    const deferred = Q.defer();
    const schema = this.schema;

    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    // default createRequired to false
    if (typeof options.createRequired === 'undefined') {
      options.createRequired = false;
    }

    // default updateTimestamps to true
    if (typeof options.updateTimestamps === 'undefined') {
      options.updateTimestamps = true;
    }

    // if the key part was empty, try the key defaults before giving up...
    if (key === null || key === undefined) {
      key = {};

      // first figure out the primary/hash key
      let hashKeyDefault = schema.attributes[schema.hashKey.name].options.default;

      if (typeof hashKeyDefault === 'undefined') {
        deferred.reject(new errors.ModelError('Key required to get item'));
        return deferred.promise.nodeify(next);
      }

      key[schema.hashKey.name] = typeof hashKeyDefault === 'function' ? hashKeyDefault() : hashKeyDefault;

      // now see if you have to figure out a range key
      if (schema.rangeKey) {
        let rangeKeyDefault = schema.attributes[schema.rangeKey.name].options.default;

        if (typeof rangeKeyDefault === 'undefined') {
          deferred.reject(new errors.ModelError('Range key required: ' + schema.rangeKey.name));
          return deferred.promise.nodeify(next);
        }

        key[schema.rangeKey.name] = typeof rangeKeyDefault === 'function' ? rangeKeyDefault() : rangeKeyDefault;
      }
    }

    const hashKeyName = schema.hashKey.name;
    if (!key[hashKeyName]) {
      const keyVal = key;
      key = {};
      key[hashKeyName] = keyVal;
    }

    const updateReq = {
      TableName: this.table.name,
      Key: {},
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      ReturnValues: 'ALL_NEW',
    };
    processCondition(updateReq, options, this.schema);

    updateReq.Key[hashKeyName] = schema.hashKey.toDynamo(key[hashKeyName]);

    if (schema.rangeKey) {
      const rangeKeyName = schema.rangeKey.name;
      updateReq.Key[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName]);
    }

    // determine the set of operations to be executed
    class Operations {
      constructor() {
        this.ifNotExistsSet = {};
        this.SET = {};
        this.ADD = {};
        this.REMOVE = {};
      }

      addIfNotExistsSet(name, item) {
        this.ifNotExistsSet[name] = item;
      }

      addSet(name, item) {
        this.SET[name] = item;
      }

      addAdd(name, item) {
        this.ADD[name] = item;
      }

      addRemove(name, item) {
        this.REMOVE[name] = item;
      }

      getUpdateExpression(updateReq) {
        let attrCount = 0;
        let updateExpression = '';

        let attrName;
        let valName;

        const setExpressions = [];
        _.forEach(this.ifNotExistsSet, (item, name) => {
          attrName = '#_n' + attrCount;
          valName = ':_p' + attrCount;
          updateReq.ExpressionAttributeNames[attrName] = name;
          updateReq.ExpressionAttributeValues[valName] = item;
          setExpressions.push(attrName + ' = if_not_exists(' + attrName + ', ' + valName + ')');
          attrCount += 1;
        });

        _.forEach(this.SET, (item, name) => {
          item = this.SET[name];
          attrName = '#_n' + attrCount;
          valName = ':_p' + attrCount;
          updateReq.ExpressionAttributeNames[attrName] = name;
          updateReq.ExpressionAttributeValues[valName] = item;
          setExpressions.push(attrName + ' = ' + valName);
          attrCount += 1;
        });
        if (setExpressions.length > 0) {
          updateExpression += 'SET ' + setExpressions.join(',') + ' ';
        }

        const addExpressions = [];
        _.forEach(this.ADD, (item, name) => {
          attrName = '#_n' + attrCount;
          valName = ':_p' + attrCount;
          updateReq.ExpressionAttributeNames[attrName] = name;
          updateReq.ExpressionAttributeValues[valName] = item;
          addExpressions.push(attrName + ' ' + valName);
          attrCount += 1;
        });
        if (addExpressions.length > 0) {
          updateExpression += 'ADD ' + addExpressions.join(',') + ' ';
        }

        const removeExpressions = [];
        _.forEach(this.REMOVE, (item, name) => {
          attrName = '#_n' + attrCount;
          updateReq.ExpressionAttributeNames[attrName] = name;
          removeExpressions.push(attrName);
          attrCount += 1;
        });
        if (removeExpressions.length > 0) {
          updateExpression += 'REMOVE ' + removeExpressions.join(',');
        }

        updateReq.UpdateExpression = updateExpression;
      }
    }

    const operations = new Operations();

    if (update.$PUT || (!update.$PUT && !update.$DELETE && !update.$ADD)) {
      const updatePUT = update.$PUT || update;

      for (let putItem of Object.keys(updatePUT)) {
        const putAttr = schema.attributes[putItem];
        if (putAttr) {
          const val = updatePUT[putItem];

          let removeParams = val === null || val === undefined || val === '';

          if (!options.allowEmptyArray) {
            removeParams = removeParams || (Array.isArray(val) && val.length === 0);
          }

          if (removeParams) {
            operations.addRemove(putItem, null);
          } else {
            try {
              operations.addSet(putItem, putAttr.toDynamo(val));
            } catch (err) {
              deferred.reject(err);
              return deferred.promise.nodeify(next);
            }
          }
        }
      }
    }

    if (update.$DELETE) {
      for (let deleteItem of Object.keys(update.$DELETE)) {
        const deleteAttr = schema.attributes[deleteItem];
        if (deleteAttr) {
          const delVal = update.$DELETE[deleteItem];
          if (delVal !== null && delVal !== undefined) {
            try {
              operations.addRemove(deleteItem, deleteAttr.toDynamo(delVal));
            } catch (err) {
              deferred.reject(err);
              return deferred.promise.nodeify(next);
            }
          } else {
            operations.addRemove(deleteItem, null);
          }
        }
      }
    }

    if (update.$ADD) {
      for (let addItem of Object.keys(update.$ADD)) {
        const addAttr = schema.attributes[addItem];
        if (addAttr) {
          try {
            operations.addAdd(addItem, addAttr.toDynamo(update.$ADD[addItem]));
          } catch (err) {
            deferred.reject(err);
            return deferred.promise.nodeify(next);
          }
        }
      }
    }

    // update schema timestamps
    if (options.updateTimestamps && schema.timestamps) {
      const createdAtLabel = schema.timestamps.createdAt;
      const updatedAtLabel = schema.timestamps.updatedAt;

      const createdAtAttribute = schema.attributes[createdAtLabel];
      const updatedAtAttribute = schema.attributes[updatedAtLabel];

      const createdAtDefaultValue = createdAtAttribute.options.default();
      const updatedAtDefaultValue = updatedAtAttribute.options.default();

      operations.addIfNotExistsSet(createdAtLabel, createdAtAttribute.toDynamo(createdAtDefaultValue));
      operations.addSet(updatedAtLabel, updatedAtAttribute.toDynamo(updatedAtDefaultValue));
    }

    // do the required items check. Throw an error if you have an item that is required and
    //  doesn't have a default.
    if (options.createRequired) {
      for (let attributeName of Object.keys(schema.attributes)) {
        const attribute = schema.attributes[attributeName];
        if (attribute.required && // if the attribute is required...
            attributeName !== schema.hashKey.name && // ...and it isn't the hash key...
            (!schema.rangeKey || attributeName !== schema.rangeKey.name) && // ...and it isn't the range key...
            (!schema.timestamps ||
                attributeName !== schema.timestamps.createdAt) && // ...and it isn't the createdAt attribute...
            (!schema.timestamps ||
                attributeName !== schema.timestamps.updatedAt) && // ...and it isn't the updatedAt attribute...
            !operations.SET[attributeName] &&
            !operations.ADD[attributeName] &&
            !operations.REMOVE[attributeName]) {
          let defaultValueOrFunction = attribute.options.default;

          // throw an error if you have required attribute without a default (and you didn't supply
          //  anything to update with)
          if (typeof defaultValueOrFunction === 'undefined') {
            const err = 'Required attribute "' + attributeName + '" does not have a default.';
            debug('Error returned by updateItem', err);
            deferred.reject(err);
            return deferred.promise.nodeify(next);
          }

          const defaultValue = typeof defaultValueOrFunction === 'function' ?
              defaultValueOrFunction() : defaultValueOrFunction;

          operations.addIfNotExistsSet(attributeName, attribute.toDynamo(defaultValue));
        }
      }
    }

    operations.getUpdateExpression(updateReq);

    // AWS doesn't allow empty expressions or attribute collections
    if (!updateReq.UpdateExpression) {
      delete updateReq.UpdateExpression;
    }
    if (!Object.keys(updateReq.ExpressionAttributeNames).length) {
      delete updateReq.ExpressionAttributeNames;
    }
    if (!Object.keys(updateReq.ExpressionAttributeValues).length) {
      delete updateReq.ExpressionAttributeValues;
    }

    const ThisModel = this;

    function updateItem() {
      debug('updateItem', updateReq);
      ThisModel.base.ddb().updateItem(updateReq, (err, data) => {
        if (err) {
          debug('Error returned by updateItem', err);
          return deferred.reject(err);
        }
        debug('updateItem response', data);

        if (!Object.keys(data).length) {
          return deferred.resolve();
        }

        const document = new ThisModel();
        schema.parseDynamo(document, data.Attributes);

        debug('updateItem parsed document', document);

        deferred.resolve(document);
      });
    }

    if (this.options.waitForActive) {
      this.table.waitForActive().then(updateItem);
    } else {
      updateItem();
    }

    return deferred.promise.nodeify(next);
  }

  delete(documentOrKey, options, next) {
    debug('Model#delete', this);
    const deferred = Q.defer();

    const ThisModel = this;
    const schema = ThisModel.schema;
    const hashKeyName = schema.hashKey.name;

    let document;

    if (documentOrKey instanceof Document) {
      document = documentOrKey;
    } else {
      let key = documentOrKey;
      if (!key[hashKeyName]) {
        const keyVal = key;
        key = {};
        key[hashKeyName] = keyVal;
      }
      if (schema.rangeKey && !key[schema.rangeKey.name]) {
        const deferred = Q.defer();
        deferred.reject(new errors.ModelError('Range key required: %s', schema.hashKey.name));
        return deferred.promise.nodeify(next);
      }
      document = new Document(key);
    }

    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    if (document[hashKeyName] === null || document[hashKeyName] === undefined) {
      deferred.reject(new errors.ModelError('Hash key required: %s', hashKeyName));
      return deferred.promise.nodeify(next);
    }

    if (schema.rangeKey &&
      (document[schema.rangeKey.name] === null || document[schema.rangeKey.name] === undefined)) {
      deferred.reject(new errors.ModelError('Range key required: %s', schema.hashKey.name));
      return deferred.promise.nodeify(next);
    }

    const getDelete = {
      TableName: ThisModel.table.name,
      Key: {},
    };

    try {
      getDelete.Key[hashKeyName] = schema.hashKey.toDynamo(document[hashKeyName]);
      if (schema.rangeKey) {
        const rangeKeyName = schema.rangeKey.name;
        getDelete.Key[rangeKeyName] = schema.rangeKey.toDynamo(document[rangeKeyName]);
      }
    } catch (err) {
      deferred.reject(err);
      return deferred.promise.nodeify(next);
    }

    if (options.update) {
      getDelete.ReturnValues = 'ALL_OLD';
      getDelete.ConditionExpression = 'attribute_exists(' + schema.hashKey.name + ')';
    }

    function deleteItem() {
      debug('deleteItem', getDelete);
      ThisModel.base.ddb().deleteItem(getDelete, (err, data) => {
        if (err) {
          debug('Error returned by deleteItem', err);
          return deferred.reject(err);
        }
        debug('deleteItem response', data);
        if (options.update && data.Attributes) {
          try {
            schema.parseDynamo(ThisModel, data.Attributes);
            debug('deleteItem parsed model', ThisModel); // TODO: or this document?
          } catch (err) {
            return deferred.reject(err);
          }
        }
        deferred.resolve(ThisModel); // TODO: or this document?
      });
    }

    if (ThisModel.options.waitForActive) {
      ThisModel.table.waitForActive().then(deleteItem);
    } else {
      deleteItem();
    }
    return deferred.promise.nodeify(next);
  }

  query(query, options, next) {
    if (typeof options === 'function') {
      next = options;
      options = null;
    }
    query = new Query(this, query, options);
    if (next) {
      query.exec(next);
    }

    return query;
  }

  queryOne(query, options, next) {
    if (typeof options === 'function') {
      next = options;
      options = null;
    }
    query = new Query(this, query, options);
    query.one();
    if (next) {
      query.exec(next);
    }

    return query;
  }

  scan(filter, options, next) {
    if (typeof options === 'function') {
      next = options;
      options = null;
    }
    const scan = new Scan(this, filter, options);
    if (next) {
      scan.exec(next);
    }

    return scan;
  }

  batchGet(keys, options, next) {
    debug('BatchGet %j', keys);
    const deferred = Q.defer();
    if (!(keys instanceof Array)) {
      deferred.reject(new errors.ModelError('batchGet requires keys to be an array'));
      return deferred.promise.nodeify(next);
    }
    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    const schema = this.schema;

    const hashKeyName = schema.hashKey.name;
    keys = keys.map((key) => {
      if (!key[hashKeyName]) {
        const ret = {};
        ret[hashKeyName] = key;
        return ret;
      }
      return key;
    });

    if (schema.rangeKey && !keys.every((key) => { return key[schema.rangeKey.name]; })) {
      deferred.reject(
          new errors.ModelError('Range key required: ' + schema.rangeKey.name)
      );
      return deferred.promise.nodeify(next);
    }

    const batchReq = {
      RequestItems: {},
    };

    const ThisModel = this;

    const getReq = {};
    batchReq.RequestItems[this.table.name] = getReq;

    getReq.Keys = keys.map((key) => {
      const ret = {};
      ret[hashKeyName] = schema.hashKey.toDynamo(key[hashKeyName]);

      if (schema.rangeKey) {
        const rangeKeyName = schema.rangeKey.name;
        ret[rangeKeyName] = schema.rangeKey.toDynamo(key[rangeKeyName]);
      }
      return ret;
    });

    if (options.attributes) {
      getReq.AttributesToGet = options.attributes;
    }

    if (options.consistent) {
      getReq.ConsistentRead = true;
    }


    function batchGet() {
      debug('batchGetItem', batchReq);
      ThisModel.base.ddb().batchGetItem(batchReq, (err, data) => {
        if (err) {
          debug('Error returned by batchGetItem', err);
          return deferred.reject(err);
        }
        debug('batchGetItem response', data);

        if (!Object.keys(data).length) {
          return deferred.resolve();
        }

        function toModel(item) {
          const document = new ThisModel();
          schema.parseDynamo(document, item);

          debug('batchGet parsed document', document);

          return document;
        }

        const models = data.Responses[ThisModel.table.name] ? data.Responses[ThisModel.table.name].map(toModel) : [];
        if (data.UnprocessedKeys[ThisModel.table.name]) {
          // convert unprocessed keys back to dynamoose format
          models.unprocessed = data.UnprocessedKeys[ThisModel.table.name].Keys.map((key) => {
            const ret = {};
            ret[hashKeyName] = schema.hashKey.parseDynamo(key[hashKeyName]);

            if (schema.rangeKey) {
              const rangeKeyName = schema.rangeKey.name;
              ret[rangeKeyName] = schema.rangeKey.parseDynamo(key[rangeKeyName]);
            }
            return ret;
          });
        }
        deferred.resolve(models);
      });
    }

    if (this.options.waitForActive) {
      this.table.waitForActive().then(batchGet);
    } else {
      batchGet();
    }
    return deferred.promise.nodeify(next);
  }

  batchPut(items, options, next) {
    debug('BatchPut %j', items);
    const deferred = Q.defer();

    if (!(items instanceof Array)) {
      deferred.reject(new errors.ModelError('batchPut requires items to be an array'));
      return deferred.promise.nodeify(next);
    }

    const model = this;

    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    const batchRequests = toBatchChunks(model.table.name, items, MAX_BATCH_WRITE_SIZE, (item) => {
      return {
        PutRequest: {
          Item: model.schema.toDynamo(item),
        },
      };
    });

    const batchPut = () => {
      batchWriteItems(model, batchRequests).then((result) => {
        deferred.resolve(result);
      }).fail((err) => {
        deferred.reject(err);
      });
    };

    // if (this.options.waitForActive) {
    //   model.table.waitForActive().then(batchPut);
    // } else {
      batchPut();
    // }
    return deferred.promise.nodeify(next);
  }

  batchDelete(keys, options, next) {
    debug('BatchDel %j', keys);
    const deferred = Q.defer();

    if (!(keys instanceof Array)) {
      deferred.reject(new errors.ModelError('batchDelete requires keys to be an array'));
      return deferred.promise.nodeify(next);
    }

    options = options || {};
    if (typeof options === 'function') {
      next = options;
      options = {};
    }

    const schema = this.schema;
    const hashKeyName = schema.hashKey.name;

    const batchRequests = toBatchChunks(this.table.name, keys, MAX_BATCH_WRITE_SIZE, (key) => {
      const keyElement = {};
      keyElement[hashKeyName] = schema.hashKey.toDynamo(key[hashKeyName]);

      if (schema.rangeKey) {
        keyElement[schema.rangeKey.name] = schema.rangeKey.toDynamo(key[schema.rangeKey.name]);
      }

      return {
        DeleteRequest: {
          Key: keyElement,
        },
      };
    });

    const ThisModel = this;

    const batchDelete = () => {
      batchWriteItems(ThisModel, batchRequests).then((result) => {
        deferred.resolve(result);
      }).fail((err) => {
        deferred.reject(err);
      });
    };

    if (this.options.waitForActive) {
      this.table.waitForActive().then(batchDelete);
    } else {
      batchDelete();
    }
    return deferred.promise.nodeify(next);
  }

  waitForActive(timeout, next) {
    return this.table.waitForActive(timeout, next);
  }
}

function processCondition(req, options, schema) {
  if (options.condition) {
    if (req.ConditionExpression) {
      req.ConditionExpression = '(' + req.ConditionExpression + ') and (' + options.condition + ')';
    } else {
      req.ConditionExpression = options.condition;
    }

    if (options.conditionNames) {
      req.ExpressionAttributeNames = {};
      for (let name of Object.keys(options.conditionNames)) {
        req.ExpressionAttributeNames['#' + name] = options.conditionNames[name];
      }
    }
    if (options.conditionValues) {
      req.ExpressionAttributeValues = {};
      Object.keys(options.conditionValues).forEach((k) => {
        const val = options.conditionValues[k];
        const attr = schema.attributes[k];
        if (attr) {
          req.ExpressionAttributeValues[':' + k] = attr.toDynamo(val);
        } else {
          throw new errors.ModelError('Invalid condition value: ' + k +
            '. The name must either be in the schema or a full DynamoDB object must be specified.');
        }
      });
    }
  }
}

function toBatchChunks(modelName, list, chunkSize, requestMaker) {
  const listClone = list.slice(0);
  let chunk;
  const batchChunks = [];

  while ((chunk = listClone.splice(0, chunkSize)).length) {
    const requests = chunk.map(requestMaker);
    const batchReq = {
      RequestItems: {},
    };

    batchReq.RequestItems[modelName] = requests;
    batchChunks.push(batchReq);
  }

  return batchChunks;
}

function reduceBatchResult(resultList) {
  return resultList.reduce((acc, res) => {
    const responses = res.Responses ? res.Responses : {};
    const unprocessed = res.UnprocessedItems ? res.UnprocessedItems : {};

    // merge responses
    for (let tableName of Object.keys(responses)) {
      if (responses.hasOwnProperty(tableName)) {
        let consumed = acc.Responses[tableName] ? acc.Responses[tableName].ConsumedCapacityUnits : 0;
        consumed += responses[tableName].ConsumedCapacityUnits;

        acc.Responses[tableName] = {
          ConsumedCapacityUnits: consumed,
        };
      }
    }

    // merge unprocessed items
    for (let tableName2 of Object.keys(unprocessed)) {
      if (unprocessed.hasOwnProperty(tableName2)) {
        const items = acc.UnprocessedItems[tableName2] ? acc.UnprocessedItems[tableName2] : [];
        items.push(unprocessed[tableName2]);
        acc.UnprocessedItems[tableName2] = items;
      }
    }

    return acc;
  }, {Responses: {}, UnprocessedItems: {}});
}

function batchWriteItems(model, batchRequests) {
  debug('batchWriteItems');

  const batchList = batchRequests.map((batchReq) => {
    const deferredBatch = Q.defer();

    model.base.ddb().batchWriteItem(batchReq, (err, data) => {
      if (err) {
        debug('Error returned by batchWriteItems', err);
        return deferredBatch.reject(err);
      }

      deferredBatch.resolve(data);
    });

    return deferredBatch.promise;
  });

  return Q.all(batchList).then((resultList) => {
    return reduceBatchResult(resultList);
  });
}

function sendErrorToCallback(error, options, next) {
  if (typeof options === 'function') {
    next = options;
  }
  if (typeof next === 'function') {
    next(error);
  }
}

const tcWrapper = function(f) {
  return function() {
    try {
      return f.apply(this, arguments);
    } catch(err) {
      let args = Array.from(arguments).slice(-2);
      let options = args[0];
      let next = args[1];
      sendErrorToCallback(err, options, next);
      return Q.reject(err);
    }
  };
};

module.exports = Model;
