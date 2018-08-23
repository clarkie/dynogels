'use strict';

const dynogels = require('../index');
const util = require('util');
const _ = require('lodash');
const async = require('async');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-query', {
  hashKey: 'name',
  rangeKey: 'email',
  timestamps: true,
  schema: {
    name: Joi.string(),
    email: Joi.string().email(),
    age: Joi.number(),
  },

  indexes: [
    { hashKey: 'name', rangeKey: 'createdAt', type: 'local', name: 'CreatedAtIndex' }
  ]
});

const printResults = (err, resp) => {
  console.log('----------------------------------------------------------------------');
  if (err) {
    console.log('Error running query', err);
  } else {
    console.log('Found', resp.Count, 'items');
    console.log(util.inspect(_.pluck(resp.Items, 'attrs')));

    if (resp.ConsumedCapacity) {
      console.log('----------------------------------------------------------------------');
      console.log('Query consumed: ', resp.ConsumedCapacity);
    }
  }

  console.log('----------------------------------------------------------------------');
};

const loadSeedData = (callback) => {
  callback = callback || _.noop;

  async.times(25, (n, next) => {
    const prefix = n % 5 === 0 ? 'foo' : 'test';
    Account.create({ email: `${prefix}${n}@example.com`, name: `Test ${n % 3}`, age: n }, next);
  }, callback);
};

const runQueries = () => {
  // Basic query against hash key
  Account.query('Test 0').exec(printResults);

  // Run query limiting returned items to 3
  Account.query('Test 0').limit(3).exec(printResults);

  // Query with rang key condition
  Account.query('Test 1')
    .where('email').beginsWith('foo')
    .exec(printResults);

  // Run query returning only email and created attributes
  // also returns consumed capacity query took
  Account.query('Test 2')
    .where('email').gte('a@example.com')
    .attributes(['email', 'createdAt'])
    .returnConsumedCapacity()
    .exec(printResults);

  // Run query against secondary index
  Account.query('Test 0')
    .usingIndex('CreatedAtIndex')
    .where('createdAt').lt(new Date().toISOString())
    .descending()
    .exec(printResults);
};

async.series([
  async.apply(dynogels.createTables.bind(dynogels)),
  loadSeedData
], (err) => {
  if (err) {
    console.log('error', err);
    process.exit(1);
  }

  runQueries();
});
