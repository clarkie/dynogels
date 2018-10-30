'use strict';

const dynogels = require('../index');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Product = dynogels.define('example-streaming-Product', {
  hashKey: 'ProductId',
  timestamps: true,
  schema: {
    ProductId: Joi.string(),
    host: Joi.string(),
    url: Joi.string(),
    title: Joi.string()
  }
});

const printStream = (msg, stream) => {
  let count = 0;
  stream.on('error', (err) => {
    console.log(`error ${msg}`, err);
  });

  stream.on('readable', () => {
    count += 1;
    console.log(`----------------------${count}--------------------------`);
    console.log(`Scanned ${stream.read().Count} products - ${msg}`);
  });

  stream.on('end', () => {
    console.log('-------------------------------------------------');
    console.log(`Finished ${msg}`);
    console.log('-------------------------------------------------');
  });
};

const s1 = Product.scan().loadAll().exec();
printStream('Loading All Accounts', s1);

const s2 = Product.scan().limit(100).loadAll().exec();
printStream('Load All Accounts 100 at a time', s2);

const totalSegments = 4;
const s3 = Product.parallelScan(totalSegments)
  .attributes('url')
  .exec();

printStream('Parallel Loaded urls', s3);
