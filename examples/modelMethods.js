'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Account = dynogels.define('example-model-methods-Account', {
  hashKey: 'email',
  timestamps: true,
  schema: {
    email: Joi.string(),
    name: Joi.string(),
    age: Joi.number(),
  }
});

Account.prototype.sayHello = function () {
  console.log(`Hello my name is ${this.get('name')} I'm ${this.get('age')} years old`);
};

Account.findByAgeRange = (low, high) => {
  Account.scan()
    .where('age').gte(low)
    .where('age').lte(high)
    .loadAll()
    .exec((err, data) => {
      data.Items.forEach((account) => {
        account.sayHello();
      });
    });
};
