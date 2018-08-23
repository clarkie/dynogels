'use strict';

const sinon = require('sinon');
const AWS = require('aws-sdk');
const Table = require('../lib/table');
const _ = require('lodash');

exports.mockDynamoDB = () => {
  const opts = { endpoint: 'http://dynamodb-local:8000', apiVersion: '2012-08-10' };
  const db = new AWS.DynamoDB(opts);

  db.scan = sinon.stub();
  db.putItem = sinon.stub();
  db.deleteItem = sinon.stub();
  db.query = sinon.stub();
  db.getItem = sinon.stub();
  db.updateItem = sinon.stub();
  db.createTable = sinon.stub();
  db.describeTable = sinon.stub();
  db.updateTable = sinon.stub();
  db.deleteTable = sinon.stub();
  db.batchGetItem = sinon.stub();
  db.batchWriteItem = sinon.stub();

  return db;
};

exports.realDynamoDB = () => {
  const opts = { endpoint: 'http://localhost:8000', apiVersion: '2012-08-10', region: 'eu-west-1' };
  return new AWS.DynamoDB(opts);
};

exports.mockDocClient = () => {
  const client = new AWS.DynamoDB.DocumentClient({ service: exports.mockDynamoDB() });

  const operations = [
    'batchGet',
    'batchWrite',
    'put',
    'get',
    'delete',
    'update',
    'scan',
    'query'
  ];

  _.each(operations, (op) => {
    client[op] = sinon.stub();
  });

  client.service.scan = sinon.stub();
  client.service.putItem = sinon.stub();
  client.service.deleteItem = sinon.stub();
  client.service.query = sinon.stub();
  client.service.getItem = sinon.stub();
  client.service.updateItem = sinon.stub();
  client.service.createTable = sinon.stub();
  client.service.describeTable = sinon.stub();
  client.service.updateTable = sinon.stub();
  client.service.deleteTable = sinon.stub();
  client.service.batchGetItem = sinon.stub();
  client.service.batchWriteItem = sinon.stub();

  return client;
};

exports.mockSerializer = () => {
  const serializer = {
    buildKey: sinon.stub(),
    deserializeItem: sinon.stub(),
    serializeItem: sinon.stub(),
    serializeItemForUpdate: sinon.stub()
  };

  return serializer;
};

exports.mockTable = () => sinon.createStubInstance(Table);

exports.fakeUUID = () => {
  const uuid = {
    v1: sinon.stub(),
    v4: sinon.stub()
  };

  return uuid;
};

exports.randomName = prefix => `${prefix}_${Date.now()}.${_.random(1000)}`;

exports.testLogger = () => ({
  info: () => null,
  warn: () => null,
});
