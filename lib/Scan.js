'use strict';

const Q = require('q');
const debug = require('debug')('dynamoose:scan');

const errors = require('./errors');

function Scan(Model, filter, options) {

  this.Model = Model;
  this.options = options || {};

  // [{
  //     name: 'name',
  //     values: ['value', ...],
  //     comparison: 'string'
  //   },
  //    ...
  // ]
  this.filters = {};
  this.buildState = false;
  this.validationError = null;

  if (typeof filter === 'string') {
    this.buildState = filter;
    this.filters[filter] = {name: filter};
  } else if (typeof filter === 'object') {
    this.parseFilterObject(filter);
  }
}

Scan.prototype.exec = function (next) {
  debug('exec scan for ', this.scan);
  if (this.validationError) {
    if (next) {
      next(this.validationError);
    }
    return Q.reject(this.validationError);
  }

  const Model = this.Model;
  const schema = Model.schema;
  const options = this.options;

  const scanReq = {
    TableName: Model.name
  };

  if (Object.keys(this.filters).length > 0) {
    scanReq.ScanFilter = {};
    for (let name in this.filters) {
      const filter = this.filters[name];
      const filterAttr = schema.attributes[name];
      scanReq.ScanFilter[name] = {
        AttributeValueList: [],
        ComparisonOperator: filter.comparison
      };

      if (filter.values) {
        for (let i = 0; i < filter.values.length; i++) {
          const val = filter.values[i];
          scanReq.ScanFilter[name].AttributeValueList.push(
            filterAttr.toDynamo(val, true)
          );
        }
      }
    }
  }

  if (options.attributes) {
    scanReq.AttributesToGet = options.attributes;
  }

  if (options.count) {
    scanReq.Select = 'COUNT';
  }

  if (options.counts) {
    scanReq.Select = 'COUNT';
  }

  if (options.limit) {
    scanReq.Limit = options.limit;
  }

  if (options.ExclusiveStartKey) {
    scanReq.ExclusiveStartKey = options.ExclusiveStartKey;
  }

  if (options.conditionalOperator) {
    scanReq.ConditionalOperator = options.conditionalOperator;
  }

  function scan() {
    const deferred = Q.defer();

    debug('scan request', scanReq);
    Model.base.ddb().scan(scanReq, function (err, data) {
      if (err) {
        debug('Error returned by scan', err);
        return deferred.reject(err);
      }
      debug('scan response', data);

      if (!Object.keys(data).length) {
        return deferred.resolve();
      }

      function toModel(item) {
        const model = new Model();
        schema.parseDynamo(model, item);

        debug('scan parsed model', model);

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
    return Model.table.waitForActive().then(scan);
  }

  return scan();
};

Scan.prototype.parseFilterObject = function (filter) {

  if (Object.keys(filter).length > 0) {

    for (let filterName in filter) {
      if (filter.hasOwnProperty(filterName)) {

        // Parse AND OR
        if (filterName === 'and' || filterName === 'or') {

          this[filterName]();
          for (let condition in filter[filterName]) {
            if (filter[filterName].hasOwnProperty(condition)) {
              this.parseFilterObject(filter[filterName][condition]);
            }
          }
        } else {

          this.where(filterName);
          let val, comp;

          if (typeof filter[filterName] === 'object' &&
            Object.keys(filter[filterName]).length === 1) {

            comp = Object.keys(filter[filterName])[0];

            if (comp === 'null') {
              if (!filter[filterName][comp]) {
                comp = 'not_null';
              }
              val = [null];
            } else if (comp === 'in' || comp === 'between') {
              val = filter[filterName][comp];
            } else {
              val = [filter[filterName][comp]];
            }

          } else {
            comp = 'eq';
            val = [filter[filterName]];
          }
          this.compVal(val, comp.toUpperCase());
        }

      }
    }
  }
};

Scan.prototype.and = function () {
  this.options.conditionalOperator = 'AND';
  return this;
};

Scan.prototype.or = function () {
  this.options.conditionalOperator = 'OR';
  return this;
};

Scan.prototype.where = function (filter) {
  if (this.validationError) {
    return this;
  }

  if (this.buildState) {
    this.validationError = new errors.ScanError('Invalid scan state; where() must follow comparison');
    return this;
  }
  if (typeof filter === 'string') {
    this.buildState = filter;
    if (this.filters[filter]) {
      this.validationError = new errors.ScanError('Invalid scan state; %s can only be used once', filter);
      return this;
    }
    this.filters[filter] = {name: filter};
  }

  return this;
};
Scan.prototype.filter = Scan.prototype.where;

Scan.prototype.compVal = function (vals, comp) {
  if (this.validationError) {
    return this;
  }

  const permittedComparison =
    [
      'NOT_NULL', 'NULL', 'EQ', 'NE', 'GE', 'LT', 'GT', 'LE', 'GE',
      'NOT_CONTAINS', 'CONTAINS', 'BEGINS_WITH', 'IN', 'BETWEEN'
    ];

  if (!this.buildState) {
    this.validationError =
      new errors.ScanError('Invalid scan state; %s must follow scan(), where(), or filter()', comp);
    return this;
  }

  if (permittedComparison.indexOf(comp) === -1) {
    this.validationError = new errors.ScanError('Invalid comparison %s', comp);
    return this;
  }

  this.filters[this.buildState].values = vals;
  this.filters[this.buildState].comparison = comp;

  this.buildState = false;
  this.notState = false;

  return this;
};

Scan.prototype.not = function () {
  this.notState = true;
  return this;
};

Scan.prototype.null = function () {
  return (this.notState) ? this.compVal(null, 'NOT_NULL') : this.compVal(null, 'NULL');
};

Scan.prototype.eq = function (val) {
  return (this.notState) ? this.compVal([val], 'NE') : this.compVal([val], 'EQ');
};

Scan.prototype.lt = function (val) {
  return (this.notState) ? this.compVal([val], 'GE') : this.compVal([val], 'LT');
};

Scan.prototype.le = function (val) {
  return (this.notState) ? this.compVal([val], 'GT') : this.compVal([val], 'LE');
};

Scan.prototype.ge = function (val) {
  return (this.notState) ? this.compVal([val], 'LT') : this.compVal([val], 'GE');
};

Scan.prototype.gt = function (val) {
  return (this.notState) ? this.compVal([val], 'LE') : this.compVal([val], 'GT');
};

Scan.prototype.contains = function (val) {
  return (this.notState) ? this.compVal([val], 'NOT_CONTAINS') : this.compVal([val], 'CONTAINS');
};

Scan.prototype.beginsWith = function (val) {
  if (this.validationError) {
    return this;
  }
  if (this.notState) {
    this.validationError = new errors.ScanError('Invalid scan state: beginsWith() cannot follow not()');
    return this;
  }
  return this.compVal([val], 'BEGINS_WITH');
};

Scan.prototype.in = function (vals) {
  if (this.validationError) {
    return this;
  }
  if (this.notState) {
    this.validationError = new errors.ScanError('Invalid scan state: in() cannot follow not()');
    return this;
  }
  return this.compVal(vals, 'IN');
};

Scan.prototype.between = function (a, b) {
  if (this.validationError) {
    return this;
  }
  if (this.notState) {
    this.validationError = new errors.ScanError('Invalid scan state: between() cannot follow not()');
    return this;
  }
  return this.compVal([a, b], 'BETWEEN');
};

Scan.prototype.limit = function (limit) {
  this.options.limit = limit;
  return this;
};

Scan.prototype.startAt = function (key) {
  this.options.ExclusiveStartKey = key;
  return this;
};

Scan.prototype.attributes = function (attributes) {
  this.options.attributes = attributes;
  return this;
};

Scan.prototype.count = function () {
  this.options.count = true;
  this.options.select = 'COUNT';
  return this;
};

Scan.prototype.counts = Scan.prototype.count;

module.exports = Scan;
