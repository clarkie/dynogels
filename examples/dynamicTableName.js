'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-tablename', {
  hashKey: 'email',
  timestamps: true,
  schema: {
    email: Joi.string(),
    name: Joi.string(),
    age: Joi.number()
  },
  tableName: function () {
    const d = new Date();
    return ['example-dynamic-tablename', d.getFullYear(), d.getMonth() + 1].join('_');
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

dynogels.createTables((err) => {
  if (err) {
    console.log('Failed to create tables', err);
  } else {
    console.log('tables created & active');
    Account.get('test@example.com', printAccountInfo);
    Account.get('foo@example.com', { ConsistentRead: true }, printAccountInfo);

    Account.create({ email: 'test@example.com', name: 'test', age: 21 }, printAccountInfo);
  }
});
