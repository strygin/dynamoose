'use strict';

class ExtendableError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
    if (typeof Error.captureStackTrace === 'function') { //noinspection
      Error.captureStackTrace(this, this.constructor);
    } else {
      this.stack = (new Error(message)).stack;
    }
  }
}

module.exports = {
  SchemaError: class SchemaError extends ExtendableError {
    constructor(message) { super(message || 'Error with schema'); }
  },
  ModelError: class ModelError extends ExtendableError {
    constructor(message) { super(message || 'Error with model'); }
  },
  QueryError: class QueryError extends ExtendableError {
    constructor(message) { super(message || 'Error with query'); }
  },
  ScanError: class ScanError extends ExtendableError {
    constructor(message) { super(message || 'Error with scan'); }
  },
  ValidationError: class ValidationError extends ExtendableError {
    constructor(message) { super(message || 'Validation error'); }
  },
};
