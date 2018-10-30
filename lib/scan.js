'use strict';

const _ = require('lodash');
const expressions = require('./expressions');
const queryBase = require('./query-base');
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

Scan.prototype = Object.create(queryBase);
Scan.prototype.constructor = Scan;

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

Scan.prototype.segments = function (segment, totalSegments) {
  this.request.Segment = segment;
  this.request.TotalSegments = totalSegments;

  return this;
};


Scan.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};

Scan.prototype.exec = function (callback) {
  const self = this;

  const runScan = (params, callback) => {
    self.table.runScan(params, callback);
  };

  return utils.paginatedRequest(self, runScan, callback);
};

Scan.prototype.buildRequest = function () {
  return _.merge({}, this.request, { TableName: this.table.tableName() });
};
