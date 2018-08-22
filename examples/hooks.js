'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-hook', {
  hashKey: 'email',
  timestamps: true,
  schema: {
    email: Joi.string().email(),
    name: Joi.string(),
    age: Joi.number(),
  }
});

Account.before('create', (data, next) => {
  if (!data.name) {
    data.name = 'Foo Bar';
  }

  return next(null, data);
});

Account.before('update', (data, next) => {
  data.age = 45;
  return next(null, data);
});

Account.after('create', (item) => {
  console.log('Account created', item.get());
});

Account.after('update', (item) => {
  console.log('Account updated', item.get());
});

Account.after('destroy', (item) => {
  console.log('Account destroyed', item.get());
});

dynogels.createTables((err) => {
  if (err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  Account.create({ email: 'test11@example.com' }, (err, acc) => {
    acc.set({ age: 25 });

    acc.update(() => {
      acc.destroy({ ReturnValues: 'ALL_OLD' });
    });
  });
});
