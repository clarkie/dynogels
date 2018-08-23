'use strict';

const helper = require('./test-helper');
const Schema = require('../lib/schema');
const Scan = require('../lib/scan');
const _ = require('lodash');
const chai = require('chai');
const Joi = require('joi');

const expect = chai.expect;

chai.should();

const internals = {};

internals.assertScanFilter = (scan, expected) => {
  const conds = _.map(scan.request.ScanFilter, c => c.format());

  if (!_.isArray(expected)) {
    expected = [expected];
  }

  conds.should.eql(expected);
};

describe('Scan', () => {
  let schema;
  let serializer;
  let table;

  beforeEach(() => {
    serializer = helper.mockSerializer();

    table = helper.mockTable();
    table.tableName = () => 'accounts';

    table.docClient = helper.mockDocClient();

    const config = {
      hashKey: 'name',
      rangeKey: 'email',
      schema: {
        name: Joi.string(),
        email: Joi.string(),
        created: Joi.date()
      },
      indexes: [{ hashKey: 'name', rangeKey: 'created', type: 'local', name: 'CreatedIndex' }]
    };

    schema = new Schema(config);
    table.schema = schema;
  });

  describe('#exec', () => {
    it('should call run scan on table', (done) => {
      table.runScan.yields(null, { ConsumedCapacity: { CapacityUnits: 5, TableName: 'accounts' }, Count: 10, ScannedCount: 12 });
      serializer.serializeItem.returns({ name: { S: 'tim' } });

      new Scan(table, serializer).exec((err, results) => {
        results.ConsumedCapacity.should.eql({ CapacityUnits: 5, TableName: 'accounts' });
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        done();
      });
    });

    it('should return LastEvaluatedKey', (done) => {
      table.runScan.yields(null, { LastEvaluatedKey: { name: 'tim' }, Count: 10, ScannedCount: 12 });
      serializer.serializeItem.returns({ name: { S: 'tim' } });

      new Scan(table, serializer).exec((err, results) => {
        results.Count.should.equal(10);
        results.ScannedCount.should.equal(12);

        results.LastEvaluatedKey.should.eql({ name: 'tim' });

        done();
      });
    });

    it('should return error', (done) => {
      table.runScan.yields(new Error('Fail'));

      new Scan(table, serializer).exec((err, results) => {
        expect(err).to.exist;
        expect(results).to.not.exist;
        done();
      });
    });

    it('should run scan after encountering a retryable exception', (done) => {
      const err = new Error('RetryableException');
      err.retryable = true;

      table.runScan
        .onCall(0).yields(err)
        .onCall(1).yields(err)
        .onCall(2).yields(null, { Items: [{ name: 'foo' }] });

      new Scan(table, serializer).exec((err, data) => {
        expect(err).to.not.exist;
        expect(data).to.exist;
        expect(data.Items).to.have.length(1);

        expect(table.runScan.calledThrice).to.be.true;
        done();
      });
    });
  });

  describe('#limit', () => {
    it('should set the limit', () => {
      const scan = new Scan(table, serializer).limit(10);

      scan.request.Limit.should.equal(10);
    });


    it('should throw when limit is zero', () => {
      const scan = new Scan(table, serializer);
      expect(() => {
        scan.limit(0);
      }).to.throw('Limit must be greater than 0');
    });
  });

  describe('#attributes', () => {
    it('should set array attributes to get', () => {
      const scan = new Scan(table, serializer).attributes(['created', 'email']);
      scan.request.ProjectionExpression.should.eql('#created,#email');
      scan.request.ExpressionAttributeNames.should.eql({ '#created': 'created', '#email': 'email' });
    });

    it('should set single attribute to get', () => {
      const scan = new Scan(table, serializer).attributes('email');
      scan.request.ProjectionExpression.should.eql('#email');
      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
    });
  });

  describe('#startKey', () => {
    it('should set start Key to hash', () => {
      const key = { name: { S: 'tim' } };
      serializer.buildKey.returns(key);

      const scan = new Scan(table, serializer).startKey('tim');

      scan.request.ExclusiveStartKey.should.eql(key);
    });

    it('should set start Key to hash + range', () => {
      const key = { name: { S: 'tim' }, email: { S: 'foo@example.com' } };
      serializer.buildKey.returns(key);

      const scan = new Scan(table, serializer).startKey({ name: 'tim', email: 'foo@example.com' });

      scan.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', () => {
    it('should set select Key', () => {
      const scan = new Scan(table, serializer).select('COUNT');

      scan.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', () => {
    it('should set return consumed capacity Key to passed in value', () => {
      const scan = new Scan(table, serializer).returnConsumedCapacity('TOTAL');
      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', () => {
      const scan = new Scan(table, serializer).returnConsumedCapacity();

      scan.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#segment', () => {
    it('should set both segment and total segments keys', () => {
      const scan = new Scan(table, serializer).segments(0, 4);

      scan.request.Segment.should.eql(0);
      scan.request.TotalSegments.should.eql(4);
    });
  });


  describe('#where', () => {
    let scan;

    beforeEach(() => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          created: Joi.date(),
          scores: Schema.types.numberSet(),
          data: Joi.object()
        },
        indexes: [{ hashKey: 'name', rangeKey: 'created', type: 'local', name: 'CreatedIndex' }]
      };

      schema = new Schema(config);
      table.schema = schema;

      scan = new Scan(table, serializer);
    });

    it('should have equals clause', () => {
      scan = scan.where('email').equals('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email = :email)');
    });

    it('should have not equals clause', () => {
      scan = scan.where('email').ne('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email <> :email)');
    });

    it('should have less than or equal clause', () => {
      scan = scan.where('email').lte('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email <= :email)');
    });

    it('should have less than clause', () => {
      scan = scan.where('email').lt('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email < :email)');
    });

    it('should have greater than or equal clause', () => {
      scan = scan.where('email').gte('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email >= :email)');
    });

    it('should have greater than clause', () => {
      scan = scan.where('email').gt('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(#email > :email)');
    });

    it('should have not null clause', () => {
      scan = scan.where('email').notNull();

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      expect(scan.request.ExpressionAttributeValues).to.not.exist;
      scan.request.FilterExpression.should.eql('(attribute_exists(#email))');
    });

    it('should have null clause', () => {
      scan = scan.where('email').null();

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      expect(scan.request.ExpressionAttributeValues).to.not.exist;
      scan.request.FilterExpression.should.eql('(attribute_not_exists(#email))');
    });

    it('should have contains clause', () => {
      scan = scan.where('email').contains('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(contains(#email, :email))');
    });

    it('should not pass a number set when making contains call', () => {
      scan = scan.where('scores').contains(2);

      scan.request.ExpressionAttributeNames.should.eql({ '#scores': 'scores' });
      scan.request.ExpressionAttributeValues.should.eql({ ':scores': 2 });
      scan.request.FilterExpression.should.eql('(contains(#scores, :scores))');
    });

    it('should have not contains clause', () => {
      scan = scan.where('email').notContains('foo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      scan.request.FilterExpression.should.eql('(NOT contains(#email, :email))');
    });

    it('should have in clause', () => {
      scan = scan.where('email').in(['foo@example.com', 'test@example.com']);

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com', ':email_2': 'test@example.com' });
      scan.request.FilterExpression.should.eql('(#email IN (:email,:email_2))');
    });

    it('should have begins with clause', () => {
      scan = scan.where('email').beginsWith('foo');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo' });
      scan.request.FilterExpression.should.eql('(begins_with(#email, :email))');
    });

    it('should have between clause', () => {
      scan = scan.where('email').between('bob@bob.com', 'foo@foo.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'bob@bob.com', ':email_2': 'foo@foo.com' });
      scan.request.FilterExpression.should.eql('(#email BETWEEN :email AND :email_2)');
    });

    it('should have multiple filters', () => {
      scan = scan
        .where('name').equals('Tim')
        .where('email').beginsWith('foo');

      scan.request.ExpressionAttributeNames.should.eql({ '#name': 'name', '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':name': 'Tim', ':email': 'foo' });
      scan.request.FilterExpression.should.eql('(#name = :name) AND (begins_with(#email, :email))');
    });

    it('should have multiple filters on the same attribute', () => {
      scan = scan
        .where('email').gt('foo@example.com')
        .where('email').lt('moo@example.com');

      scan.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      scan.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com', ':email_2': 'moo@example.com' });
      scan.request.FilterExpression.should.eql('(#email > :email) AND (#email < :email_2)');
    });

    it('should convert date to iso string', () => {
      const d = new Date();
      scan = scan.where('created').equals(d);

      scan.request.ExpressionAttributeNames.should.eql({ '#created': 'created' });
      scan.request.ExpressionAttributeValues.should.eql({ ':created': d.toISOString() });
      scan.request.FilterExpression.should.eql('(#created = :created)');
    });

    it('should support Map.Attr document paths', () => {
      scan = scan.where('data.attr').equals(15);

      scan.request.ExpressionAttributeNames.should.eql({ '#data': 'data', '#attr': 'attr' });
      scan.request.ExpressionAttributeValues.should.eql({ ':data_attr': 15 });
      scan.request.FilterExpression.should.eql('(#data.#attr = :data_attr)');
    });
  });

  describe('#loadAll', () => {
    it('should set load all option to true', () => {
      const scan = new Scan(table, serializer).loadAll();

      scan.options.loadAll.should.be.true;
    });
  });


  describe('#filterExpression', () => {
    it('should set filter expression', () => {
      const scan = new Scan(table, serializer).filterExpression('Postedby = :val');
      scan.request.FilterExpression.should.equal('Postedby = :val');
    });
  });

  describe('#expressionAttributeValues', () => {
    it('should set expression attribute values', () => {
      const scan = new Scan(table, serializer).expressionAttributeValues({ ':val': 'test' });
      scan.request.ExpressionAttributeValues.should.eql({ ':val': 'test' });
    });
  });

  describe('#expressionAttributeNames', () => {
    it('should set expression attribute names', () => {
      const scan = new Scan(table, serializer).expressionAttributeNames({ '#name': 'name' });
      scan.request.ExpressionAttributeNames.should.eql({ '#name': 'name' });
    });
  });

  describe('#projectionExpression', () => {
    it('should set projection expression', () => {
      const scan = new Scan(table, serializer).projectionExpression('#name, #email');
      scan.request.ProjectionExpression.should.eql('#name, #email');
    });
  });
});
