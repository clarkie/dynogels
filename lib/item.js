'use strict';

const util = require('util');
const _ = require('lodash');
const events = require('events');

const internals = {};

internals.identity = () => {};

const Item = module.exports = function (attrs, table) {
  events.EventEmitter.call(this);

  this.table = table;

  this.set(attrs || {});
};

util.inherits(Item, events.EventEmitter);

Item.prototype.get = function (key) {
  if (key) {
    return this.attrs[key];
  } else {
    return this.attrs;
  }
};

Item.prototype.set = function (paramsOrKey, value) {
  if (_.isString(paramsOrKey)) {
    this.attrs[paramsOrKey] = value;
  } else {
    this.attrs = _.merge({}, this.attrs, paramsOrKey);
  }

  return this;
};

Item.prototype.save = function (callback) {
  const self = this;
  callback = callback || internals.identity;

  self.table.create(this.attrs, (err, item) => {
    if (err) {
      return callback(err);
    }

    self.set(item.attrs);

    return callback(null, item);
  });
};

Item.prototype.update = function (options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  options = options || {};
  callback = callback || internals.identity;

  self.table.update(this.attrs, options, (err, item) => {
    if (err) {
      return callback(err);
    }

    if (item) {
      self.set(item.attrs);
    }

    return callback(null, item);
  });
};

Item.prototype.destroy = function (options, callback) {
  const self = this;

  if (typeof options === 'function' && !callback) {
    callback = options;
    options = {};
  }

  options = options || {};
  callback = callback || internals.identity;

  self.table.destroy(this.attrs, options, callback);
};

Item.prototype.toJSON = function () {
  return _.cloneDeep(this.attrs);
};

Item.prototype.toPlainObject = function () {
  return _.cloneDeep(this.attrs);
};
