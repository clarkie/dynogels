'use strict';

const dynogels = require('../index');
const AWS = dynogels.AWS;
const Joi = require('joi');

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

  next(null, data);
});

Account.before('update', (data, next) => {
  data.age = 45;
  next(null, data);
});

Account.after('create', (item, next) => {
  console.log('Account created', item.get());
  next(null, item);
});

Account.after('update', (item, next) => {
  console.log('Account updated', item.get());
  next(null, item);
});

Account.after('destroy', (item, next) => {
  console.log('Account destroyed', item.get());
  next(null, item);
});

dynogels.createTables(err => {
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
