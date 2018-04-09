'use strict';

const dynogels = require('../index');
const async = require('async');
const _ = require('lodash');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-batch-get-account', {
  hashKey: 'email',
  timestamps: true,
  schema: {
    email: Joi.string().email(),
    name: Joi.string(),
    age: Joi.number(),
    roles: dynogels.types.stringSet()
  }
});

const printAccountInfo = (err, acc) => {
  if (err) {
    console.log('got error', err);
  } else if (acc) {
    console.log('got account', acc.get());
  } else {
    console.log('account not found');
  }
};

const loadSeedData = (callback) => {
  callback = callback || _.noop;

  async.times(15, (n, next) => {
    const roles = n % 3 === 0 ? ['admin', 'editor'] : ['user'];
    Account.create({ email: `test${n}@example.com`, name: `Test ${n % 3}`, age: n, roles: roles }, next);
  }, callback);
};

async.series([
  async.apply(dynogels.createTables.bind(dynogels)),
  loadSeedData
], (err) => {
  if (err) {
    console.log('error', err);
    process.exit(1);
  }

  // Get two accounts at once
  Account.getItems(['test1@example.com', 'test2@example.com'], (err, accounts) => {
    accounts.forEach((acc) => {
      printAccountInfo(null, acc);
    });
  });

  // Same as above but a strongly consistent read is used
  Account.getItems(['test3@example.com', 'test4@example.com'], { ConsistentRead: true }, (err, accounts) => {
    accounts.forEach((acc) => {
      printAccountInfo(null, acc);
    });
  });

  // Get two accounts, but only fetching the age attribute
  Account.getItems(['test5@example.com', 'test6@example.com'], { AttributesToGet: ['age'] }, (err, accounts) => {
    accounts.forEach((acc) => {
      printAccountInfo(null, acc);
    });
  });
});
