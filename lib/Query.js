'use strict';

const Q = require('q');
const errors = require('./errors');
const debug = require('debug')('dynamoose:query');

const VALID_RANGE_KEYS = ['EQ', 'LE', 'LT', 'GE', 'GT', 'BEGINS_WITH', 'BETWEEN'];

class Query {

  constructor(Model, query, options) {
    this.Model = Model;
    this.options = options || {};
    this.query = {hashKey: {}};

    this.filters = {};
    this.buildState = false;
    this.validationError = null;
    this.counts = Query.prototype.count;

    let hashKeyName;
    let hashKeyVal;

    if (typeof query === 'string') {
      this.buildState = 'hashKey';
      this.query.hashKey.name = query;
    } else if (query.hash) {
      hashKeyName = Object.keys(query.hash)[0];
      hashKeyVal = query.hash[hashKeyName];
      if (hashKeyVal.eq !== null && hashKeyVal.eq !== undefined) {
        hashKeyVal = hashKeyVal.eq;
      }
      this.query.hashKey.name = hashKeyName;
      this.query.hashKey.value = hashKeyVal;

      if (query.range) {
        const rangeKeyName = Object.keys(query.range)[0];
        let rangeKeyVal = query.range[rangeKeyName];
        const rangeKeyComp = Object.keys(rangeKeyVal)[0];
        rangeKeyVal = rangeKeyVal[rangeKeyComp];
        this.query.rangeKey = {
          name: rangeKeyName,
          value: rangeKeyVal,
          comparison: rangeKeyComp
        };
      }
    } else {
      hashKeyName = Object.keys(query)[0];
      hashKeyVal = query[hashKeyName];
      if (hashKeyVal.eq !== null && hashKeyVal.eq !== undefined) {
        hashKeyVal = hashKeyVal.eq;
      }
      this.query.hashKey.name = hashKeyName;
      this.query.hashKey.value = hashKeyVal;
    }
  }

  exec(next) {
    debug('exec query for ', this.query);
    if (this.validationError) {
      if (next) {
        next(this.validationError);
      }
      return Q.reject(this.validationError);
    }

    const Model = this.Model;
    const schema = Model.schema;
    const options = this.options;

    debug('Query with schema', schema);

    const queryReq = {
      TableName: Model.name,
      KeyConditions: {}
    };

    let indexName, index;
    if (schema.hashKey.name !== this.query.hashKey.name) {
      debug('query is on global secondary index');
      for (indexName in schema.indexes.global) {
        index = schema.indexes.global[indexName];
        if (index.name === this.query.hashKey.name) {
          debug('using index', indexName);
          queryReq.IndexName = indexName;
          break;
        }
      }

    }

    const hashAttr = schema.attributes[this.query.hashKey.name];

    queryReq.KeyConditions[this.query.hashKey.name] = {
      AttributeValueList: [hashAttr.toDynamo(this.query.hashKey.value)],
      ComparisonOperator: 'EQ'
    };

    let i, val;

    if (this.query.rangeKey) {
      let rangeKey = this.query.rangeKey;
      const rangeAttr = schema.attributes[rangeKey.name];

      if (!queryReq.IndexName && schema.rangeKey.name !== rangeKey.name) {
        debug('query is on local secondary index');
        for (indexName in schema.indexes.local) {
          index = schema.indexes.local[indexName];
          if (index.name === rangeKey.name) {
            debug('using local index', indexName);
            queryReq.IndexName = indexName;
            break;
          }
        }
      }

      if (!rangeKey || rangeKey.values === undefined) {
        debug('No range key value (i.e. get all)');
      } else {
        debug('Range key: %s', rangeKey.name);
        const keyConditions = queryReq.KeyConditions[rangeKey.name] = {
          AttributeValueList: [],
          ComparisonOperator: rangeKey.comparison.toUpperCase()
        };
        for (i = 0; i < rangeKey.values.length; i++) {
          val = rangeKey.values[i];
          keyConditions.AttributeValueList.push(
              rangeAttr.toDynamo(val, true)
          );
        }
      }
    }

    if (this.filters && Object.keys(this.filters).length > 0) {
      queryReq.QueryFilter = {};
      for (let name in this.filters) {
        const filter = this.filters[name];
        const filterAttr = schema.attributes[name];
        queryReq.QueryFilter[name] = {
          AttributeValueList: [],
          ComparisonOperator: filter.comparison.toUpperCase()
        };

        if (filter.values) {
          for (i = 0; i < filter.values.length; i++) {
            val = filter.values[i];
            queryReq.QueryFilter[name].AttributeValueList.push(
                filterAttr.toDynamo(val, true)
            );
          }
        }
      }
    }

    if (options.or) {
      queryReq.ConditionalOperator = 'OR'; // defualts to AND
    }

    if (options.attributes) {
      queryReq.AttributesToGet = options.attributes;
    }

    if (options.count) {
      queryReq.Select = 'COUNT';
    }

    if (options.counts) {
      queryReq.Select = 'COUNT';
    }

    if (options.consistent) {
      queryReq.ConsistentRead = true;
    }

    if (options.limit) {
      queryReq.Limit = options.limit;
    }

    if (options.one) {
      queryReq.Limit = 1;
    }

    if (options.descending) {
      queryReq.ScanIndexForward = false;
    }

    if (options.ExclusiveStartKey) {
      queryReq.ExclusiveStartKey = options.ExclusiveStartKey;
    }

    function query() {
      const deferred = Q.defer();

      debug('DynamoDB Query: %j', queryReq);
      Model.base.ddb().query(queryReq, (err, data) => {
        if (err) {
          debug('Error returned by query', err);
          return deferred.reject(err);
        }
        debug('DynamoDB Query Response: %j', data);

        if (!Object.keys(data).length) {
          return deferred.resolve();
        }

        function toModel(item) {
          const model = new Model();
          schema.parseDynamo(model, item);

          debug('query parsed model', model);

          return model;
        }

        try {
          let models = {};
          if (options.count) {
            return deferred.resolve(data.Count);
          }
          if (options.counts) {
            const counts = {count: data.Count, scannedCount: data.ScannedCount};
            return deferred.resolve(counts);
          }
          if (data.Items !== undefined) {
            models = data.Items.map(toModel);
            if (options.one) {
              if (!models || models.length === 0) {
                return deferred.resolve();
              }
              return deferred.resolve(models[0]);
            }
            models.lastKey = data.LastEvaluatedKey;
          }
          models.count = data.Count;
          models.scannedCount = data.ScannedCount;
          deferred.resolve(models);
        } catch (err) {
          deferred.reject(err);
        }

      });

      return deferred.promise.nodeify(next);
    }

    if (Model.options.waitForActive) {
      return Model.table.waitForActive().then(query);
    }

    return query();
  }

  where(rangeKey) {
    if (this.validationError) {
      return this;
    }
    if (this.buildState) {
      this.validationError = new errors.QueryError('Invalid query state; where() must follow eq()');
      return this;
    }
    if (typeof rangeKey === 'string') {
      this.buildState = 'rangeKey';
      this.query.rangeKey = {name: rangeKey};
    } else {
      const rangeKeyName = Object.keys(rangeKey)[0];
      let rangeKeyVal = rangeKey[rangeKeyName];
      const rangeKeyComp = Object.keys(rangeKeyVal)[0];
      rangeKeyVal = rangeKeyVal[rangeKeyComp];
      this.query.rangeKey = {
        name: rangeKeyName,
        values: [rangeKeyVal],
        comparison: rangeKeyComp
      };
    }

    return this;
  }

  filter(filter) {
    if (this.validationError) {
      return this;
    }
    if (this.buildState) {
      this.validationError = new errors.QueryError('Invalid query state; filter() must follow comparison');
      return this;
    }
    if (typeof filter === 'string') {
      this.buildState = 'filter';
      this.currentFilter = filter;
      if (this.filters[filter]) {
        this.validationError = new errors.QueryError('Invalid query state; %s filter can only be used once', filter);
        return this;
      }
      this.filters[filter] = {name: filter};
    }

    return this;
  }

  compVal(vals, comp) {
    if (this.validationError) {
      return this;
    }
    if (this.buildState === 'hashKey') {
      if (comp !== 'EQ') {
        this.validationError = new errors.QueryError('Invalid query state; eq must follow query()');
        return this;
      }
      this.query.hashKey.value = vals[0];
    } else if (this.buildState === 'rangeKey') {
      if (VALID_RANGE_KEYS.indexOf(comp) < 0) {
        this.validationError = new errors.QueryError('Invalid query state; %s must follow filter()', comp);
        return this;
      }
      this.query.rangeKey.values = vals;
      this.query.rangeKey.comparison = comp;
    } else if (this.buildState === 'filter') {
      this.filters[this.currentFilter].values = vals;
      this.filters[this.currentFilter].comparison = comp;
    } else {
      this.validationError =
        new errors.QueryError('Invalid query state; %s must follow query(), where() or filter()', comp);
      return this;
    }

    this.buildState = false;
    this.notState = false;

    return this;
  }

  and() {
    this.options.or = false;
    return this;
  }

  or() {
    this.options.or = true;
    return this;
  }

  not() {
    this.notState = true;
    return this;
  }

  null() {
    return (this.notState) ? this.compVal(null, 'NOT_NULL') : this.compVal(null, 'NULL');
  }

  eq(val) {
    return (this.notState) ? this.compVal([val], 'NE') : this.compVal([val], 'EQ');
  }

  lt(val) {
    return (this.notState) ? this.compVal([val], 'GE') : this.compVal([val], 'LT');
  }

  le(val) {
    return (this.notState) ? this.compVal([val], 'GT') : this.compVal([val], 'LE');
  }

  ge(val) {
    return (this.notState) ? this.compVal([val], 'LT') : this.compVal([val], 'GE');
  }

  gt(val) {
    return (this.notState) ? this.compVal([val], 'LE') : this.compVal([val], 'GT');
  }

  contains(val) {
    return (this.notState) ? this.compVal([val], 'NOT_CONTAINS') : this.compVal([val], 'CONTAINS');
  }

  beginsWith(val) {
    if (this.validationError) {
      return this;
    }
    if (this.notState) {
      this.validationError = new errors.QueryError('Invalid Query state: beginsWith() cannot follow not()');
      return this;
    }
    return this.compVal([val], 'BEGINS_WITH');
  }

  in(vals) {
    if (this.validationError) {
      return this;
    }
    if (this.notState) {
      this.validationError = new errors.QueryError('Invalid Query state: in() cannot follow not()');
      return this;
    }
    return this.compVal(vals, 'IN');
  }

  between(a, b) {
    if (this.validationError) {
      return this;
    }
    if (this.notState) {
      this.validationError = new errors.QueryError('Invalid Query state: between() cannot follow not()');
      return this;
    }
    return this.compVal([a, b], 'BETWEEN');
  }

  limit(limit) {
    this.options.limit = limit;
    return this;
  }

  one() {
    this.options.one = true;
    return this;
  }

  consistent() {
    this.options.consistent = true;
    return this;
  }

  descending() {
    this.options.descending = true;
    return this;
  }

  ascending() {
    this.options.descending = false;
    return this;
  }

  startAt(key) {
    this.options.ExclusiveStartKey = key;
    return this;
  }

  attributes(attributes) {
    this.options.attributes = attributes;
    return this;
  }

  count() {
    this.options.count = true;
    this.options.select = 'COUNT';
    return this;
  }

}

module.exports = Query;
