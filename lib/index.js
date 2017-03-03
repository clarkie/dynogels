'use strict';

const _ = require('lodash');
const util = require('util');
const AWS = require('aws-sdk');
const DocClient = AWS.DynamoDB.DocumentClient;
const Table = require('./table');
const Schema = require('./schema');
const serializer = require('./serializer');
const batch = require('./batch');
const Item = require('./item');
const createTables = require('./createTables');
const bunyan = require('bunyan');

const dynogels = module.exports;

dynogels.AWS = AWS;

const internals = {};

dynogels.log = bunyan.createLogger({
  name: 'dynogels',
  serializers: { err: bunyan.stdSerializers.err },
  level: bunyan.FATAL
});

dynogels.dynamoDriver = internals.dynamoDriver = driver => {
  if (driver) {
    internals.dynamodb = driver;

    const docClient = internals.loadDocClient(driver);
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.dynamodb = internals.dynamodb || new dynogels.AWS.DynamoDB({ apiVersion: '2012-08-10' });
  }

  return internals.dynamodb;
};

dynogels.documentClient = internals.documentClient = docClient => {
  if (docClient) {
    internals.docClient = docClient;
    internals.dynamodb = docClient.service;
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.loadDocClient();
  }

  return internals.docClient;
};

internals.updateDynamoDBDocClientForAllModels = docClient => {
  _.each(dynogels.models, model => {
    model.config({ docClient: docClient });
  });
};

internals.loadDocClient = driver => {
  if (driver) {
    internals.docClient = new DocClient({ service: driver });
  } else {
    internals.docClient = internals.docClient || new DocClient({ service: internals.dynamoDriver() });
  }

  return internals.docClient;
};

function extend(protoProps, staticProps) {
  const Parent = this;

  // The constructor function for the new subclass is either defined by you
  // (the "constructor" property in your `extend` definition), or defaulted
  // by us to simply call the parent's constructor.
  const Child = protoProps && protoProps.hasOwnProperty('constructor')
    ? protoProps.constructor
    : function () { return Parent.apply(this, arguments); };

  _.assign(Child, Parent, staticProps);

  // Set the prototype chain to inherit from `Parent`.
  Child.prototype = Object.create(Parent.prototype, {
    constructor: {
      value: Child,
      enumerable: false,
      writable: true,
      configurable: true
    }
  });

  if (protoProps) {
    _.assign(Child.prototype, protoProps);
  }

  // Give child access to the parent prototype as part of "super"
  Child.__super__ = Parent.prototype;

  return Child;
}

internals.compileModel = (name, schema) => {
  // extremly simple table names
  const tableName = `${name.toLowerCase()}s`;

  const log = dynogels.log.child({ model: name });

  const table = new Table(tableName, schema, serializer, internals.loadDocClient(), log);

  const Model = function (attrs) {
    Item.call(this, attrs, table);
  };

  util.inherits(Model, Item);

  Model.get = _.bind(table.get, table);
  Model.create = _.bind(table.create, table);
  Model.update = _.bind(table.update, table);
  Model.destroy = _.bind(table.destroy, table);
  Model.query = _.bind(table.query, table);
  Model.scan = _.bind(table.scan, table);
  Model.parallelScan = _.bind(table.parallelScan, table);

  Model.getItems = batch(table, serializer).getItems;
  Model.batchGetItems = batch(table, serializer).getItems;

  Model.extend = extend;

  // table ddl methods
  Model.createTable = _.bind(table.createTable, table);
  Model.updateTable = _.bind(table.updateTable, table);
  Model.describeTable = _.bind(table.describeTable, table);
  Model.deleteTable = _.bind(table.deleteTable, table);
  Model.tableName = _.bind(table.tableName, table);

  table.itemFactory = Model;

  Model.log = log;

  // hooks
  Model.after = _.bind(table.after, table);
  Model.before = _.bind(table.before, table);

  Model.__defineGetter__('docClient', () => table.docClient);

  Model.config = config => {
    config = config || {};

    if (config.tableName) {
      table.config.name = config.tableName;
    }

    if (config.docClient) {
      table.docClient = config.docClient;
    } else if (config.dynamodb) {
      table.docClient = new DocClient({ service: config.dynamodb });
    }

    return table.config;
  };

  return dynogels.model(name, Model);
};

internals.addModel = (name, model) => {
  dynogels.models[name] = model;

  return dynogels.models[name];
};

dynogels.reset = () => {
  dynogels.models = {};
};

dynogels.Set = function () {
  return internals.docClient.createSet.apply(internals.docClient, arguments);
};

dynogels.define = (modelName, config) => {
  if (_.isFunction(config)) {
    throw new Error('define no longer accepts schema callback, migrate to new api');
  }

  const schema = new Schema(config);

  const compiledTable = internals.compileModel(modelName, schema);

  return compiledTable;
};

dynogels.model = (name, model) => {
  if (model) {
    internals.addModel(name, model);
  }

  return dynogels.models[name] || null;
};

dynogels.createTables = (options, callback) => {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  callback = callback || _.noop;
  options = options || {};

  return createTables(dynogels.models, options, callback);
};

dynogels.types = Schema.types;

dynogels.reset();
