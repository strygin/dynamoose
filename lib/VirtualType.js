'use strict';

const debug = require('debug')('dynamoose:virtualtype');

class VirtualType {
  /**
   * VirtualType constructor
   *
   * This is what mongoose uses to define virtual attributes via `Schema.prototype.virtual`.
   *
   * ####Example:
   *
   *     var fullname = schema.virtual('fullname');
   *     fullname instanceof mongoose.VirtualType // true
   *
   * @parma {Object} options
   * @api public
   */
  constructor(options, name) {
    this.path = name;
    this.options = options || {};
  }

  /**
   * Defines a getter.
   *
   * ####Example:
   *
   *     var virtual = schema.virtual('fullname');
   *     virtual.get(function () {
   *       return this.name.first + ' ' + this.name.last;
   *     });
   *
   * @param {Function} fn
   * @return {VirtualType} this
   * @api public
   */
  get(fn) {
    debug('registering getter for ' + this.path);
    this.getter = fn;
    return this;
  }

  /**
   * Defines a setter.
   *
   * ####Example:
   *
   *     var virtual = schema.virtual('fullname');
   *     virtual.set(function (v) {
   *       var parts = v.split(' ');
   *       this.name.first = parts[0];
   *       this.name.last = parts[1];
   *     });
   *
   * @param {Function} fn
   * @return {VirtualType} this
   * @api public
   */
  set(fn) {
    debug('registering setter for ' + this.path);
    this.setter = fn;
    return this;
  }

  /**
   * Applies getters and setters to the model
   * @param {Object} model
   * @return {any} the value after applying all getters
   * @api public
   */
  applyVirtuals(model) {
    debug('applyVirtuals for %s', this.path);
    const property = {enumerable: true, configurable: true};

    if (this.setter) {
      property.set = this.setter;
    }
    if (this.getter) {
      property.get = this.getter;
    }

    Object.defineProperty(model, this.path, property);
  }
}

module.exports = VirtualType;
