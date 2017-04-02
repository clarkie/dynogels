'use strict';

const Table = require('../lib/table');
const ParallelScan = require('../lib/parallelScan');
const Schema = require('../lib/schema');
const chai = require('chai');
const expect = chai.expect;
const assert = require('assert');
const helper = require('./test-helper');
const serializer = require('../lib/serializer');
const Joi = require('joi');

chai.should();

describe('ParallelScan', () => {
  let table;

  beforeEach(() => {
    const config = {
      hashKey: 'num',
      schema: {
        num: Joi.number(),
        name: Joi.string()
      }
    };

    const schema = new Schema(config);

    table = new Table('mockTable', schema, serializer, helper.mockDynamoDB());
  });

  it('should return error', done => {
    const scan = new ParallelScan(table, serializer, 4);

    table.docClient.scan.yields(new Error('fail'));

    scan.exec((err, data) => {
      expect(err).to.exist;
      expect(data).to.not.exist;

      return done();
    });
  });

  it('should stream error', done => {
    const scan = new ParallelScan(table, serializer, 4);

    table.docClient.scan.yields(new Error('fail'));

    const stream = scan.exec();

    stream.on('error', err => {
      expect(err).to.exist;
      return done();
    });

    stream.on('readable', () => {
      assert(false, 'readable should not be called');
    });
  });
});
