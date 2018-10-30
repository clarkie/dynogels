'use strict';

const async = require('async');
const _ = require('lodash');

const internals = {};

internals.createTable = (model, globalOptions, options, callback) => {
  globalOptions = globalOptions || {};
  options = options || {};

  const tableName = model.tableName();

  model.describeTable((err, data) => {
    if (_.isNull(data) || _.isUndefined(data)) {
      model.log.info('creating table: %s', tableName);
      return model.createTable(options, (error) => {
        if (error) {
          model.log.warn({ err: error }, 'failed to create table %s: %s', tableName, error);
          return callback(error);
        }

        model.log.info('waiting for table: %s to become ACTIVE', tableName);
        internals.waitTillActive(globalOptions, model, callback);
      });
    } else {
      model.updateTable((err) => {
        if (err) {
          model.log.warn({ err: err }, 'failed to update table %s: %s', tableName, err);
          return callback(err);
        }

        model.log.info('waiting for table: %s to become ACTIVE', tableName);
        internals.waitTillActive(globalOptions, model, callback);
      });
    }
  });
};

internals.waitTillActive = (options, model, callback) => {
  let status = 'PENDING';

  async.doWhilst(
    (callback) => {
      model.describeTable((err, data) => {
        if (err) {
          return callback(err);
        }

        status = data.Table.TableStatus;

        setTimeout(callback, options.pollingInterval || 1000);
      });
    },
    () => status !== 'ACTIVE',
    err => callback(err)
  );
};

module.exports = (models, config, callback) => {
  async.eachSeries(_.keys(models), (key, callback) => internals.createTable(models[key], config.$dynogels, config[key], callback), callback);
};
