'use strict';

const debug = require('debug')('dynamoose:table');
const Q = require('q');
const _ = require('lodash');
require('es-nodeify');

class Table {
  constructor(name, schema, options, base) {
    debug('new Table (%s)', name, schema);
    this.name = name;
    this.schema = schema;
    this.options = options || {};
    this.base = base;

    if (this.options.create === undefined || this.options.create === null) {
      this.options.create = true;
    }
  }

  deleteIndex(indexName) {
    const deferred = Q.defer();

    const table = this;
    table.active = false;
    const params = {
      TableName: table.name,
      GlobalSecondaryIndexUpdates: [
        {
          Delete: {
            IndexName: indexName
          }
        }
      ]
    };
    table.base.ddb().updateTable(params, (err, data) => {
      debug('deleteIndex handler running');
      if (err) {
        deferred.reject(err);
      }
      else {
        setTimeout(() => {
          table.waitForActive()
              .then(() => {
                deferred.resolve(data);
              });
        }, 300);
      }
    });

    return deferred.promise;
  }

  createIndex(attributes, indexSpec) {
    const deferred = Q.defer();

    const table = this;
    table.active = false;
    const params = {
      TableName: this.name,
      AttributeDefinitions: attributes,
      GlobalSecondaryIndexUpdates: [
        {
          Create: indexSpec
        }
      ]
    };

    this.base.ddb().updateTable(params, (err, data) => {
      if (err) {
        deferred.reject(err);
      }
      else {
        setTimeout(() => {
          table.waitForActive()
              .then(() => {
                deferred.resolve(data);
              });
        }, 300);
      }
    });

    return deferred.promise;
  }

  init(next) {
    debug('initializing table, %s, %j', this.name, this.options);

    const deferred = Q.defer();
    const table = this;
    let localTableReq;

    if (this.options.create) {
      this.describe()
          .then(data => {
            debug('table exist -- initialization done');
            localTableReq = this.buildTableReq(table.name, table.schema);
            const indexes = this.compareIndexes(localTableReq, data.Table);

            if (table.options.update) {
              debug('checking indexes');
              for (let idx of indexes.delete) {
                table.deleteIndex(idx.IndexName);
              }
              indexes.both.forEach(idx => {
                table.deleteIndex(idx.IndexName)
                  .then(() => {
                    table.createIndex(localTableReq.AttributeDefinitions, idx);
                  });
                });
              for (let idx of indexes.create) {
                table.createIndex(localTableReq.AttributeDefinitions, idx);
              }
            } else {
              if (indexes.delete.length > 0 || indexes.create.length > 0) {
                debug('indexes are not synchronized and update flag is set to false');
                deferred.reject(new Error('indexes are not synchronized and update flag is set to false'));
              }
            }
            table.initialized = true;
            return table.waitForActive()
              .then(() => {
                //table.active = data.Table.TableStatus === 'ACTIVE';

                return deferred.resolve();
              });
          })
          .catch(err => {
            if (err && err.code === 'ResourceNotFoundException') {
              debug('table does not exist -- creating');
              return deferred.resolve(
                table.create()
                  .then(() => {
                    table.initialized = true;
                  })
                  .then(() => {
                    if (table.options.waitForActive) {
                      return table.waitForActive();
                    }
                  })
              );
            }
            if (err) {
              debug('error initializing', err.stack);
              return deferred.reject(err);
            }
          });
    } else {
      table.initialized = true;
      return deferred.resolve();
    }

    return deferred.promise.nodeify(next);
  }

  waitForActive(timeout, next) {
    debug('Waiting for Active table, %s, %j', this.name, this.options);

    const deferred = Q.defer();

    if (typeof timeout === 'function') {
      next = timeout;
      timeout = null;
    }

    if (!timeout) {
      timeout = this.options.waitForActiveTimeout;
    }

    const table = this;

    const timeoutAt = Date.now() + timeout;

    function waitForActive() {
      debug('Waiting...');
      /*
       if (table.active) {
       debug('Table flag is set to Active - %s', table.name);
       return deferred.resolve();
       }*/
      if (Date.now() > timeoutAt) {
        return deferred.reject(
          new Error('Wait for Active timed out after ' + timeout + ' ms.')
        );
      }
      if (!table.initialized) {
        return setTimeout(waitForActive, 10);
      }
      table.describe()
        .then(data => {
          let active = (data.Table.TableStatus === 'ACTIVE');
          const indexes = data.Table.GlobalSecondaryIndexes || [];
          indexes.forEach(gsi => {
            //debug('waitForActive Index Check: %s', JSON.stringify(gsi, null, 2));
            debug('index %s.IndexStatus is %s', gsi.IndexName, gsi.IndexStatus);
            if (gsi.IndexStatus !== 'ACTIVE') {
              active = false;
            }
          });
          if (!active) {
            debug('Waiting for Active again - %s', table.name);
            setTimeout(waitForActive, 500);
          } else {
            table.active = true;
            deferred.resolve();
          }
        })
        .catch(err => {
          if (err && err.code === 'ResourceNotFoundException') {
            return setTimeout(waitForActive, 10);
          }
          debug('Error waiting for active', err.stack);
          return deferred.reject(err);
        });
    }

    waitForActive();

    return deferred.promise.nodeify(next);
  }

  describe(next) {
    const describeTableReq = {
      TableName: this.name
    };

    const deferred = Q.defer();

    const ddb = this.base.ddb();
    ddb.describeTable(describeTableReq, (err, data) => {
      if (err) {
        debug('error describing table', err);
        return deferred.reject(err);
      }
      deferred.resolve(data);
    });

    return deferred.promise.nodeify(next);
  }

  buildTableReq(name, schema) {
    const attrDefs = [];

    const keyAttr = {};

    function addKeyAttr(attr) {
      if (attr) {
        keyAttr[attr.name] = attr;
      }
    }

    addKeyAttr(schema.hashKey);
    addKeyAttr(schema.rangeKey);
    for (let globalIndexName of Object.keys(schema.indexes.global)) {
      addKeyAttr(schema.indexes.global[globalIndexName]);

      // add the range key to the attribute definitions if specified
      const rangeKeyName = schema.indexes.global[globalIndexName].indexes[globalIndexName].rangeKey;
      addKeyAttr(schema.attributes[rangeKeyName]);
    }
    for (let indexName of Object.keys(schema.indexes.local)) {
      addKeyAttr(schema.indexes.local[indexName]);
    }

    _.forEach(keyAttr, (value, key) => {
      attrDefs.push({
        AttributeName: key,
        AttributeType: value.type.dynamo
      });
    });

    const keySchema = [{
      AttributeName: schema.hashKey.name,
      KeyType: 'HASH'
    }];
    if (schema.rangeKey) {
      keySchema.push({
        AttributeName: schema.rangeKey.name,
        KeyType: 'RANGE'
      });
    }

    const provThroughput = {
      ReadCapacityUnits: schema.throughput.read,
      WriteCapacityUnits: schema.throughput.write
    };

    const createTableReq = {
      AttributeDefinitions: attrDefs,
      TableName: name,
      KeySchema: keySchema,
      ProvisionedThroughput: provThroughput
    };

    debug('Creating table local indexes', schema.indexes.local);
    const localSecIndexes = [];
    let index;
    for (let localSecIndexName of Object.keys(schema.indexes.local)) {

      const indexAttr = schema.indexes.local[localSecIndexName];
      index = indexAttr.indexes[localSecIndexName];
      const localSecIndex = {
        IndexName: localSecIndexName,
        KeySchema: [{
          AttributeName: schema.hashKey.name,
          KeyType: 'HASH'
        }, {
          AttributeName: indexAttr.name,
          KeyType: 'RANGE'
        }]
      };

      if (index.project) {
        if (_.isArray(index.project)) {
          localSecIndex.Projection = {
            ProjectionType: 'INCLUDE',
            NonKeyAttributes: index.project
          };
        } else {
          localSecIndex.Projection = {
            ProjectionType: 'ALL'
          };
        }
      } else {
        localSecIndex.Projection = {
          ProjectionType: 'KEYS_ONLY'
        };
      }

      localSecIndexes.push(localSecIndex);
    }

    const globalSecIndexes = [];
    for (let globalSecIndexName of Object.keys(schema.indexes.global)) {
      const globalIndexAttr = schema.indexes.global[globalSecIndexName];
      index = globalIndexAttr.indexes[globalSecIndexName];

      const globalSecIndex = {
        IndexName: globalSecIndexName,
        KeySchema: [{
          AttributeName: globalIndexAttr.name,
          KeyType: 'HASH'
        }],
        ProvisionedThroughput: {
          ReadCapacityUnits: index.throughput.read,
          WriteCapacityUnits: index.throughput.write
        }
      };

      if (index.rangeKey) {
        globalSecIndex.KeySchema.push({
          AttributeName: index.rangeKey,
          KeyType: 'RANGE'
        });
      }

      if (index.project) {
        if (_.isArray(index.project)) {
          globalSecIndex.Projection = {
            ProjectionType: 'INCLUDE',
            NonKeyAttributes: index.project
          };
        } else {
          globalSecIndex.Projection = {
            ProjectionType: 'ALL'
          };
        }
      } else {
        globalSecIndex.Projection = {
          ProjectionType: 'KEYS_ONLY'
        };
      }
      globalSecIndexes.push(globalSecIndex);
    }

    if (!_.isEmpty(localSecIndexes)) {
      createTableReq.LocalSecondaryIndexes = localSecIndexes;
    }

    if (!_.isEmpty(globalSecIndexes)) {
      createTableReq.GlobalSecondaryIndexes = globalSecIndexes;
    }

    return createTableReq;
  }

  create(next) {
    const ddb = this.base.ddb();
    const schema = this.schema;
    const createTableReq = this.buildTableReq(this.name, schema);

    debug('ddb.createTable request:', createTableReq);

    const deferred = Q.defer();

    ddb.createTable(createTableReq, (err, data) => {
      if (err) {
        debug('error creating table', err);
        return deferred.reject(err);
      }
      debug('table created', data);
      deferred.resolve(data);
    });

    return deferred.promise.nodeify(next);
  }

  //noinspection ReservedWordAsName
  delete(next) {
    const deleteTableReq = {
      TableName: this.name
    };

    debug('ddb.deleteTable request:', deleteTableReq);

    const ddb = this.base.ddb();
    const deferred = Q.defer();

    ddb.deleteTable(deleteTableReq, (err, data) => {
      if (err) {
        debug('error deleting table', err);
        return deferred.reject(err);
      }
      debug('deleted table', data);
      deferred.resolve(data);
    });

    return deferred.promise.nodeify(next);
  }

  update(next) {
    // var ddb = this.base.ddb();
    // ddb.updateTable();
    // TODO: implement Table.update
    const deferred = Q.defer();
    deferred.reject(new Error('TODO'));

    return deferred.promise.nodeify(next);
  }

  compareIndexes(local, remote) {
    const indexes = {
      delete: [],
      create: [],
      both: []
    };

    const solid_fields = ['IndexName', 'KeySchema', 'Projection', 'ProvisionedThroughput'];
    const localIndexes = _.map(local.GlobalSecondaryIndexes, item => {
      return _.pick(item, solid_fields);
    });
    const remoteIndexes = _.map(remote.GlobalSecondaryIndexes, item => {
      return _.pick(item, solid_fields);
    });

    debug('compareIndexes');
    indexes.create = _.differenceBy(localIndexes, remoteIndexes, 'IndexName');
    indexes.delete = _.differenceBy(remoteIndexes, localIndexes, 'IndexName');
    indexes.both = _.intersectionWith(localIndexes, remoteIndexes, (localIndex, remoteIndex) => {
      if (localIndex.IndexName !== remoteIndex.IndexName) {
        return false;
      }
      if (remoteIndex.hasOwnProperty('ProvisionedThroughput')) {
        delete remote.ProvisionedThroughput.NumberOfDecreasesToday;
      }
      return !_.isEqual(localIndex, remoteIndex);
    });

    return indexes;
  }

}

module.exports = Table;