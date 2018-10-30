'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-update', {
  hashKey: 'email',
  timestamps: true,
  schema: {
    email: Joi.string().email(),
    name: Joi.string(),
    age: Joi.number(),
    nicknames: dynogels.types.stringSet(),
    nested: Joi.object()
  }
});

dynogels.createTables((err) => {
  if (err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  Account.update({ email: 'test5@example.com', age: { $add: 1 } }, (err, acc) => {
    console.log('incremented age', acc.get('age'));
  });

  Account.update({ email: 'test@example.com', nicknames: { $add: 'smalls' } }, (err, acc) => {
    console.log('added one nickname', acc.get('nicknames'));
  });

  Account.update({ email: 'test@example.com', nicknames: { $add: ['bigs', 'big husk', 'the dude'] } }, (err, acc) => {
    console.log('added three nicknames', acc.get('nicknames'));
  });

  Account.update({ email: 'test@example.com', nicknames: { $del: 'the dude' } }, (err, acc) => {
    console.log('removed nickname', acc.get('nicknames'));
  });

  Account.update({ email: 'test@example.com', nested: { roles: ['guest'] } }, (err, acc) => {
    console.log('added nested data', acc.get('nested'));
  });
});
