'use strict';

const _ = require('lodash');
const util = require('util');
const AWS = require('aws-sdk');
const Table = require('./table');
const Schema = require('./schema');
const serializer = require('./serializer');
const batch = require('./batch');
const Item = require('./item');
const createTables = require('./createTables');

const DocClient = AWS.DynamoDB.DocumentClient;
const dynogels = module.exports;

dynogels.AWS = AWS;

const internals = {};

dynogels.log = dynogels.log || {
  info: () => null,
  warn: () => null,
};

dynogels.dynamoDriver = internals.dynamoDriver = (driver) => {
  if (driver) {
    internals.dynamodb = driver;

    const docClient = internals.loadDocClient(driver);
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.dynamodb = internals.dynamodb || new dynogels.AWS.DynamoDB({ apiVersion: '2012-08-10' });
  }

  return internals.dynamodb;
};

dynogels.documentClient = internals.documentClient = (docClient) => {
  if (docClient) {
    internals.docClient = docClient;
    internals.dynamodb = docClient.service;
    internals.updateDynamoDBDocClientForAllModels(docClient);
  } else {
    internals.loadDocClient();
  }

  return internals.docClient;
};

internals.updateDynamoDBDocClientForAllModels = (docClient) => {
  _.each(dynogels.models, (model) => {
    model.config({ docClient: docClient });
  });
};

internals.loadDocClient = (driver) => {
  if (driver) {
    internals.docClient = new DocClient({ service: driver });
  } else {
    internals.docClient = internals.docClient || new DocClient({ service: internals.dynamoDriver() });
  }

  return internals.docClient;
};

internals.compileModel = (name, schema, log) => {
  const tableName = name.toLowerCase();

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
  Model.validate = _.bind(table.validate, table);

  Model.getItems = batch(table, serializer).getItems;
  Model.batchGetItems = batch(table, serializer).getItems;

  // table ddl methods
  Model.createTable = _.bind(table.createTable, table);
  Model.updateTable = _.bind(table.updateTable, table);
  Model.describeTable = _.bind(table.describeTable, table);
  Model.deleteTable = _.bind(table.deleteTable, table);
  Model.tableName = _.bind(table.tableName, table);
  Model.dynamoCreateTableParams = _.bind(table.dynamoCreateTableParams, table);

  table.itemFactory = Model;

  Model.log = log;

  // hooks
  Model.after = _.bind(table.after, table);
  Model.before = _.bind(table.before, table);

  Object.defineProperties(Model, {
    docClient: { get: () => table.docClient },
    schema: { get: () => table.schema }
  });

  Model.config = (config) => {
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

  const compiledTable = internals.compileModel(modelName, schema, config.log || dynogels.log);

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
