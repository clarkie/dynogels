'use strict';

const dynogels = require('../index');
const _ = require('lodash');
const Joi = require('joi');
const async = require('async');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Product = dynogels.define('example-parallel-scan', {
  hashKey: 'id',
  timestamps: true,
  schema: {
    id: dynogels.types.uuid(),
    accountId: Joi.number(),
    purchased: Joi.boolean().default(false),
    price: Joi.number()
  },
});

const printInfo = (err, resp) => {
  if (err) {
    console.log(err);
    return;
  }

  console.log('Count', resp.Count);
  console.log('Scanned Count', resp.ScannedCount);

  const totalPrices = resp.Items.reduce((total, item) => {
    total += item.get('price');
    return total;
  }, 0);

  console.log('Total purchased', totalPrices);
  console.log('Average purchased price', totalPrices / resp.Count);
};

const loadSeedData = (callback) => {
  callback = callback || _.noop;

  async.times(30, (n, next) => {
    const purchased = n % 4 === 0;
    Product.create({ accountId: n % 5, purchased: purchased, price: n }, next);
  }, callback);
};

const runParallelScan = () => {
  const totalSegments = 8;

  Product.parallelScan(totalSegments)
    .where('purchased').equals(true)
    .attributes('price')
    .exec(printInfo);
};

async.series([
  async.apply(dynogels.createTables.bind(dynogels)),
  loadSeedData
], (err) => {
  if (err) {
    console.log('error', err);
    process.exit(1);
  }

  runParallelScan();
});

