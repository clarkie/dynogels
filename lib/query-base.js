'use strict';

const _ = require('lodash');

module.exports = {
  limit(num) {
    if (num <= 0) {
      throw new Error('Limit must be greater than 0');
    }

    this.request.Limit = num;

    return this;
  },

  filterExpression(expression) {
    this.request.FilterExpression = expression;

    return this;
  },

  expressionAttributeValues(data) {
    this.request.ExpressionAttributeValues = data;

    return this;
  },

  expressionAttributeNames(data) {
    this.request.ExpressionAttributeNames = data;

    return this;
  },

  projectionExpression(data) {
    this.request.ProjectionExpression = data;

    return this;
  },

  startKey(hashKey, rangeKey) {
    this.request.ExclusiveStartKey = this.serializer.buildKey(hashKey, rangeKey, this.table.schema);

    return this;
  },

  attributes(attrs) {
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
  },

  select(value) {
    this.request.Select = value;

    return this;
  },

  returnConsumedCapacity(value) {
    if (_.isUndefined(value)) {
      value = 'TOTAL';
    }

    this.request.ReturnConsumedCapacity = value;

    return this;
  },

  loadAll() {
    this.options.loadAll = true;

    return this;
  },
};
