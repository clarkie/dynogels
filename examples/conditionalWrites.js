'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;

dynogels.dynamoDriver(new AWS.DynamoDB({
  endpoint: 'http://localhost:8000',
  region: 'eu-west-1',
}));

const Account = dynogels.define('example-Account', {
  hashKey: 'AccountId',
  timestamps: true,
  schema: {
    AccountId: Joi.string(),
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

  const n = 2001;
  const account = { AccountId: `${n}`, name: `Account ${n}`, email: `account${n}@gmail.com`, age: n };

  Account.create(account, (err, a1) => {
    console.log(err || a1.attrs);
    const params = {};
    params.ConditionExpression = '#i <> :x';
    params.ExpressionAttributeNames = { '#i': 'age' };
    params.ExpressionAttributeValues = { ':x': n };

    Account.create(account, params, (err, a2) => {
      console.log(err || a2.attrs);
    });
  });
});
