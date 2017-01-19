'use strict';

class ExtendableError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
    this.message = message;
    if (typeof Error.captureStackTrace === 'function') {
      Error.captureStackTrace(this, this.constructor);
    } else {
      this.stack = (new Error(message)).stack;
    }
  }
}

module.exports.SchemaError = class SchemaError extends ExtendableError {
  constructor(message) { super(message || 'Error with schema'); }
};
module.exports.ModelError = class ModelError extends ExtendableError {
  constructor(message) { super(message || 'Error with model'); }
};
module.exports.QueryError = class QueryError extends ExtendableError {
  constructor(message) { super(message || 'Error with query'); }
};
module.exports.ScanError = class ScanError extends ExtendableError {
  constructor(message) { super(message || 'Error with scan'); }
};
module.exports.ValidationError = class ValidationError extends ExtendableError {
  constructor(message) { super(message || 'Validation error'); }
};
