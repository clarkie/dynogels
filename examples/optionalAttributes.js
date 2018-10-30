'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Person = dynogels.define('example-optional-attribute', {
  hashKey: 'id',
  schema: {
    id: dynogels.types.uuid(),
    name: Joi.string().allow(null)
  }
});

const printInfo = (err, person) => {
  if (err) {
    console.log('got error', err);
  } else if (person) {
    console.log('got person', person.get());
  } else {
    console.log('person not found');
  }
};

dynogels.createTables((err) => {
  if (err) {
    console.log('Failed to create table', err);
    process.exit(1);
  }

  Person.create({ name: 'Nick' }, printInfo);
  Person.create({ name: null }, printInfo);

  const p = new Person({ name: null });
  p.save(printInfo);
});
