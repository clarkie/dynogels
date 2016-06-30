'use strict';

const vogels = require('../index');
const fs = require('fs');
const AWS = vogels.AWS;
const Joi = require('joi');

AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const BinModel = vogels.define('example-binary', {
  hashKey: 'name',
  timestamps: true,
  schema: {
    name: Joi.string(),
    data: Joi.binary()
  }
});

const printFileInfo = (err, file) => {
  if (err) {
    console.log('got error', err);
  } else if (file) {
    console.log('got file', file.get());
  } else {
    console.log('file not found');
  }
};

vogels.createTables(err => {
  if (err) {
    console.log('Error creating tables', err);
    process.exit(1);
  }

  fs.readFile(`${__dirname}/basic.js`, (err, data) => {
    if (err) {
      throw err;
    }

    BinModel.create({ name: 'basic.js', data: data }, printFileInfo);
  });
});
