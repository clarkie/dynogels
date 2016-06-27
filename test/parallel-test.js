'use strict';

var Table = require('../lib/table');
var ParallelScan = require('../lib/parallelScan');
var Schema = require('../lib/schema');
var chai = require('chai');
var expect = chai.expect;
var assert = require('assert');
var helper = require('./test-helper');
var serializer = require('../lib/serializer');
var Joi = require('joi');

chai.should();

describe('ParallelScan', function () {
  var table;

  beforeEach(function () {
    var config = {
      hashKey: 'num',
      schema: {
        num: Joi.number(),
        name: Joi.string()
      }
    };

    var schema = new Schema(config);

    table = new Table('mockTable', schema, serializer, helper.mockDynamoDB(), helper.testLogger());
  });

  it('should return error', function (done) {
    var scan = new ParallelScan(table, serializer, 4);

    table.docClient.scan.yields(new Error('fail'));

    scan.exec(function (err, data) {
      expect(err).to.exist;
      expect(data).to.not.exist;

      return done();
    });
  });

  it('should stream error', function (done) {
    var scan = new ParallelScan(table, serializer, 4);

    table.docClient.scan.yields(new Error('fail'));

    var stream = scan.exec();

    stream.on('error', function (err) {
      expect(err).to.exist;
      return done();
    });

    stream.on('readable', function () {
      assert(false, 'readable should not be called');
    });
  });
});
