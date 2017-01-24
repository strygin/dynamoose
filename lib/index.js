'use strict';

const Schema = require('./Schema');
const Model = require('./Model');

const debug = require('debug')('dynamoose');

class Dynamoose {
  constructor() {
    this.models = {};
    this.AWS = require('aws-sdk');
    this.Schema = Schema;
    this.Table = require('./Table');
    this.VirtualType = require('./VirtualType');

    this.defaults = {
      create: true,
      waitForActive: true, // Wait for table to be created
      waitForActiveTimeout: 180000, // 3 minutes
      prefix: ''
    };
  }

  model(name, schema, options) {
    options = options || {};

    for (let key of Object.keys(this.defaults)) {
      options[key] = (typeof options[key] === 'undefined') ? this.defaults[key] : options[key];
    }

    name = options.prefix + name;

    debug('Looking up model %s', name);

    if (this.models[name]) {
      return this.models[name];
    }
    if (!(schema instanceof Schema)) {
      schema = new Schema(schema, options);
    }

    const model = new Model(name, schema, options, this);
    this.models[name] = model;
    return model;
  }

  local(url) {
    this.endpointURL = url || 'http://localhost:8000';
    debug('Setting DynamoDB to local (%s)', this.endpointURL);
  }

  ddb() {
    if (this.dynamoDB) {
      return this.dynamoDB;
    }
    if (this.endpointURL) {
      debug('Setting DynamoDB to %s', this.endpointURL);
      this.dynamoDB = new this.AWS.DynamoDB({endpoint: new this.AWS.Endpoint(this.endpointURL)});
    } else {
      debug('Getting default DynamoDB');
      this.dynamoDB = new this.AWS.DynamoDB();
    }
    return this.dynamoDB;
  }

  setDefaults(options) {
    for (let key of Object.keys(this.defaults)) {
      options[key] = (typeof options[key] === 'undefined') ? this.defaults[key] : options[key];
    }
    this.defaults = options;
  }

}

module.exports = new Dynamoose();
