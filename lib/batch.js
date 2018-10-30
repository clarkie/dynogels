'use strict';

const _ = require('lodash');
const async = require('async');

const internals = {};

internals.buildInitialGetItemsRequest = (tableName, keys, options) => {
  const request = {};

  request[tableName] = _.merge({}, { Keys: keys }, options);

  return { RequestItems: request };
};

internals.serializeKeys = (keys, table, serializer) => keys.map(key => serializer.buildKey(key, null, table.schema));

internals.mergeResponses = (tableName, responses) => {
  const base = {
    Responses: {},
    ConsumedCapacity: []
  };

  base.Responses[tableName] = [];

  return responses.reduce((memo, resp) => {
    if (resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedRequest = (request, table, callback) => {
  const responses = [];

  const moreKeysToProcessFunc = () => request !== null && !_.isEmpty(request);

  const doFunc = (callback) => {
    table.runBatchGetItems(request, (err, resp) => {
      if (err && err.retryable) {
        return callback();
      } else if (err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      if (moreKeysToProcessFunc()) {
        request = { RequestItems: request };
      }
      responses.push(resp);

      return callback();
    });
  };

  const resulsFunc = (err) => {
    if (err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, moreKeysToProcessFunc, resulsFunc);
};

internals.buckets = (keys) => {
  const buckets = [];

  while (keys.length) {
    buckets.push(keys.splice(0, 100));
  }

  return buckets;
};

internals.initialBatchGetItems = (keys, table, serializer, options, callback) => {
  const serializedKeys = internals.serializeKeys(keys, table, serializer);

  const request = internals.buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  internals.paginatedRequest(request, table, (err, data) => {
    if (err) {
      return callback(err);
    }

    const dynamoItems = data.Responses[table.tableName()];

    const items = _.map(dynamoItems, i => table.initItem(serializer.deserializeItem(i)));

    return callback(null, items);
  });
};

internals.getItems = (table, serializer) => (keys, options, callback) => {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  async.map(internals.buckets(_.clone(keys)), (key, callback) => {
    internals.initialBatchGetItems(key, table, serializer, options, callback);
  }, (err, results) => {
    if (err) {
      return callback(err);
    }

    return callback(null, _.flatten(results));
  });
};

module.exports = (table, serializer) => ({
  getItems: internals.getItems(table, serializer)
});
