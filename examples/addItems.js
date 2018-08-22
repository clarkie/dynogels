'use strict';

const dynogels = require('../index');
const Joi = require('joi');
const async = require('async');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-Account', {
  hashKey: 'AccountId',
  timestamps: true,
  schema: {
    AccountId: dynogels.types.uuid(),
    name: Joi.string(),
    email: Joi.string().email(),
    age: Joi.number(),
  }
});

dynogels.createTables({
  'example-Account': { readCapacity: 1, writeCapacity: 10 },
}, (err) => {
  if (err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  async.times(25, (n, next) => {
    Account.create({ name: `Account ${n}`, email: `account${n}@gmail.com`, age: n }, next);
  });
});
