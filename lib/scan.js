'use strict';

const _ = require('lodash');
const expressions = require('./expressions');
const utils = require('./utils');

const internals = {};

internals.keyCondition = (keyName, schema, scan) => {
  const f = operator => function () {
    const copy = [].slice.call(arguments);
    const existingValueKeys = _.keys(scan.request.ExpressionAttributeValues);
    const args = [keyName, operator, existingValueKeys].concat(copy);
    const cond = expressions.buildFilterExpression.apply(null, args);
    return scan.addFilterCondition(cond);
  };

  return {
    equals: f('='),
    eq: f('='),
    ne: f('<>'),
    lte: f('<='),
    lt: f('<'),
    gte: f('>='),
    gt: f('>'),
    null: f('attribute_not_exists'),
    notNull: f('attribute_exists'),
    contains: f('contains'),
    notContains: f('NOT contains'),
    in: f('IN'),
    beginsWith: f('begins_with'),
    between: f('BETWEEN')
  };
};

const Scan = module.exports = function (table, serializer) {
  this.table = table;
  this.serializer = serializer;
  this.options = { loadAll: false };

  this.request = {};
};

Scan.prototype.limit = function (num) {
  if (num <= 0) {
    throw new Error('Limit must be greater than 0');
  }

  this.request.Limit = num;

  return this;
};

Scan.prototype.addFilterCondition = function (condition) {
  const expressionAttributeNames = _.merge({}, condition.attributeNames, this.request.ExpressionAttributeNames);
  const expressionAttributeValues = _.merge({}, condition.attributeValues, this.request.ExpressionAttributeValues);

  if (!_.isEmpty(expressionAttributeNames)) {
    this.request.ExpressionAttributeNames = expressionAttributeNames;
  }

  if (!_.isEmpty(expressionAttributeValues)) {
    this.request.ExpressionAttributeValues = expressionAttributeValues;
  }

  if (_.isString(this.request.FilterExpression)) {
    this.request.FilterExpression = `${this.request.FilterExpression} AND (${condition.statement})`;
  } else {
    this.request.FilterExpression = `(${condition.statement})`;
  }

  return this;
};

Scan.prototype.startKey = function (hashKey, rangeKey) {
  this.request.ExclusiveStartKey = this.serializer.buildKey(hashKey, rangeKey, this.table.schema);

  return this;
};

Scan.prototype.attributes = function (attrs) {
  if (!_.isArray(attrs)) {
    attrs = [attrs];
  }

  const expressionAttributeNames = _.reduce(attrs, (result, attr) => {
    const path = `#${attr}`;
    result[path] = attr;

    return result;
  }, {});

  this.request.ProjectionExpression = _.keys(expressionAttributeNames).join(',');
  this.request.ExpressionAttributeNames = _.merge({}, expressionAttributeNames, this.request.ExpressionAttributeNames);

  return this;
};

Scan.prototype.select = function (value) {
  this.request.Select = value;

  return this;
};

Scan.prototype.consumeThroughput = function (throughput) {
  if (typeof throughput !== 'number') {
    throw new Error('throughput must be number');
  }
  this.options.consumeThroughputLimit = throughput;
  this.returnConsumedCapacity();
  return this;
};

Scan.prototype.returnConsumedCapacity = function (value) {
  if (_.isUndefined(value)) {
    value = 'TOTAL';
  }

  this.request.ReturnConsumedCapacity = value;

  return this;
};

Scan.prototype.segments = function (segment, totalSegments) {
  this.request.Segment = segment;
  this.request.TotalSegments = totalSegments;

  return this;
};


Scan.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};


Scan.prototype.filterExpression = function (expression) {
  this.request.FilterExpression = expression;

  return this;
};

Scan.prototype.expressionAttributeValues = function (data) {
  this.request.ExpressionAttributeValues = data;

  return this;
};

Scan.prototype.expressionAttributeNames = function (data) {
  this.request.ExpressionAttributeNames = data;

  return this;
};

Scan.prototype.projectionExpression = function (data) {
  this.request.ProjectionExpression = data;

  return this;
};

Scan.prototype.exec = function (callback) {
  const self = this;

  const runScan = (params, callback) => {
    self.table.runScan(params, callback);
  };

  return utils.paginatedRequest(self, runScan, callback);
};

Scan.prototype.loadAll = function () {
  this.options.loadAll = true;

  return this;
};

Scan.prototype.buildRequest = function () {
  return _.merge({}, this.request, { TableName: this.table.tableName() });
};
