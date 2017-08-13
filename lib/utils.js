'use strict';

const _ = require('lodash');
const async = require('async');
const AWS = require('aws-sdk');
const Readable = require('stream').Readable;

const utils = module.exports;

utils.omitNulls = data => _.omitBy(data, value => _.isNull(value) ||
  _.isUndefined(value) ||
  (_.isArray(value) && _.isEmpty(value)) ||
  (_.isString(value) && _.isEmpty(value)));

utils.mergeResults = (responses, tableName) => {
  const result = {
    Items: [],
    ConsumedCapacity: {
      CapacityUnits: 0,
      TableName: tableName
    },
    Count: 0,
    ScannedCount: 0
  };

  const merged = _.reduce(responses, (memo, resp) => {
    if (!resp) {
      return memo;
    }

    memo.Count += resp.Count || 0;
    memo.ScannedCount += resp.ScannedCount || 0;

    if (resp.ConsumedCapacity) {
      memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0;
    }

    if (resp.Items) {
      memo.Items = memo.Items.concat(resp.Items);
    }

    memo.LastEvaluatedKey = resp.LastEvaluatedKey;

    return memo;
  }, result);

  if (!merged.LastEvaluatedKey) {
    delete merged.LastEvaluatedKey;
  }

  if (merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity;
  }

  if (merged.ScannedCount === 0) {
    delete merged.ScannedCount;
  }

  return merged;
};

utils.paginatedRequest = (self, runRequestFunc, callback) => {
  // if callback isn't passed switch to stream
  if (!callback) {
    return utils.streamRequest(self, runRequestFunc);
  }

  let lastEvaluatedKey = null;
  const responses = [];
  let retry = false;

  const doFunc = callback => {
    if (lastEvaluatedKey) {
      self.startKey(lastEvaluatedKey);
    }

    const delayableRunFunc = () => {
      Promise.resolve()
        .then(() => {
          if (self.rateLimiter) {
            return self.rateLimiter.preAuth();
          }
          return true;
        })
        .then(preAuthResult => {
          // check again
          if (!preAuthResult) {
            setTimeout(delayableRunFunc, 20);
            return;
          }
          runRequestFunc(self.buildRequest(), (err, resp) => {
            if (err) {
              retry = !!err.retryable;
              return setImmediate(callback, retry ? null : err);
            }

            retry = false;
            lastEvaluatedKey = resp.LastEvaluatedKey;

            responses.push(resp);

            const consumedCapacity = resp.ConsumedCapacity;
            Promise.resolve()
              .then(() => {
                // rate limiter enabled
                if (consumedCapacity && self.rateLimiter) {
                  return self.rateLimiter.clearTransaction(consumedCapacity);
                }
              })
              .then(() => setImmediate(callback));
          });
        });
    };
    delayableRunFunc();
  };

  const testFunc = () => (self.options.loadAll && lastEvaluatedKey) || retry;

  const resulesFunc = err => {
    if (err) {
      return callback(err);
    }
    return callback(null, utils.mergeResults(responses, self.table.tableName()));
  };

  async.doWhilst(doFunc, testFunc, resulesFunc);
}
;


utils.streamRequest = (self, runRequestFunc) => {
  let lastEvaluatedKey = null;
  let performRequest = true;

  const stream = new Readable({ objectMode: true });

  const startRead = () => {
    if (!performRequest) {
      return;
    }
    Promise.resolve()
      .then(() => {
        if (self.rateLimiter) {
          return self.rateLimiter.preAuth();
        }
        return true;
      })
      .then(preAuthResult => {
        // check again
        if (!preAuthResult) {
          setTimeout(startRead, 20);
          return;
        }
        if (lastEvaluatedKey) {
          self.startKey(lastEvaluatedKey);
        }
        runRequestFunc(self.buildRequest(), (err, resp) => {
          if (err && err.retryable) {
            return setTimeout(startRead, 1000);
          } else if (err) {
            return stream.emit('error', err);
          } else {
            lastEvaluatedKey = resp.LastEvaluatedKey;

            if (!self.options.loadAll || !lastEvaluatedKey) {
              performRequest = false;
            }
            const consumedCapacity = resp.ConsumedCapacity;
            Promise.resolve()
              .then(() => {
                // rate limiter enabled
                if (consumedCapacity && self.rateLimiter) {
                  return self.rateLimiter.clearTransaction(consumedCapacity.CapacityUnits);
                }
              })
              .then(() => {
                stream.push(resp);

                if (!self.options.loadAll || !lastEvaluatedKey) {
                  stream.push(null);
                }
              });
          }
        });
      });
  };

  stream._read = () => {
    startRead();
  };

  return stream;
};

utils.omitPrimaryKeys = (schema, params) => _.omit(params, schema.hashKey, schema.rangeKey);

utils.strToBin = value => {
  if (typeof(value) !== 'string') {
    const StrConversionError = 'Need to pass in string primitive to be converted to binary.';
    throw new Error(StrConversionError);
  }

  if (AWS.util.isBrowser()) {
    const len = value.length;
    const bin = new Uint8Array(new ArrayBuffer(len));
    for (let i = 0; i < len; i++) {
      bin[i] = value.charCodeAt(i);
    }
    return bin;
  } else {
    return AWS.util.Buffer(value);
  }
};
