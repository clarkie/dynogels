'use strict';

const dynogels = require('../index');
const util = require('util');
const _ = require('lodash');
const async = require('async');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-scan', {
  hashKey: 'name',
  rangeKey: 'email',
  timestamps: true,
  schema: {
    name: Joi.string(),
    email: Joi.string().email(),
    age: Joi.number(),
    scores: dynogels.types.numberSet(),
  },
});

const printResults = (err, resp) => {
  console.log('----------------------------------------------------------------------');
  if (err) {
    console.log('Error running scan', err);
  } else {
    console.log('Found', resp.Count, 'items');
    console.log(util.inspect(_.pluck(resp.Items, 'attrs')));

    if (resp.ConsumedCapacity) {
      console.log('----------------------------------------------------------------------');
      console.log('Scan consumed: ', resp.ConsumedCapacity);
    }
  }

  console.log('----------------------------------------------------------------------');
};

const loadSeedData = (callback) => {
  callback = callback || _.noop;

  async.times(30, (n, next) => {
    const scores = n % 5 === 0 ? [3, 4, 5] : [1, 2];
    Account.create({ email: `test${n}@example.com`, name: `Test ${n % 3}`, age: n, scores: scores }, next);
  }, callback);
};


const runScans = () => {
  // Basic scan against table
  Account.scan().exec(printResults);

  // Run scan limiting returned items to 4
  Account.scan().limit(4).exec(printResults);

  // Scan with key condition
  Account.scan()
    .where('email').beginsWith('test5')
    .exec(printResults);

  // Run scan returning only email and created attributes
  // also returns consumed capacity the scan took
  Account.scan()
    .where('email').gte('f@example.com')
    .attributes(['email', 'createdAt'])
    .returnConsumedCapacity()
    .exec(printResults);

  Account.scan()
    .where('scores').contains(2)
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

  runScans();
});
