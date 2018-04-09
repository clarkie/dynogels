'use strict';

const helper = require('./test-helper');
const Schema = require('../lib/schema');
const Query = require('../lib//query');
const Serializer = require('../lib/serializer');
const Table = require('../lib/table');
const chai = require('chai');
const assert = require('assert');
const Joi = require('joi');

const expect = chai.expect;

chai.should();

describe('Query', () => {
  let serializer;
  let table;

  beforeEach(() => {
    serializer = helper.mockSerializer();

    table = helper.mockTable();
    table.config = { name: 'accounts' };
    table.docClient = helper.mockDocClient();
  });

  describe('#exec', () => {
    it('should run query against table', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      table.schema = new Schema(config);

      table.runQuery.yields(null, {});
      serializer.serializeItem.returns({ name: { S: 'tim' } });

      new Query('tim', table, serializer).exec((err, results) => {
        results.should.eql({ Items: [], Count: 0 });
        done();
      });
    });

    it('should return error', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);
      const t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

      t.docClient.query.yields(new Error('Fail'));

      new Query('tim', t, Serializer).exec((err, results) => {
        expect(err).to.exist;
        expect(results).to.not.exist;
        done();
      });
    });

    it('should stream error', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      const t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

      t.docClient.query.yields(new Error('Fail'));

      const stream = new Query('tim', t, Serializer).exec();

      stream.on('error', (err) => {
        expect(err).to.exist;
        return done();
      });

      stream.on('readable', () => {
        assert(false, 'readable should not be called');
      });
    });

    it('should stream data after handling retryable error', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      const t = new Table('accounts', s, Serializer, helper.mockDocClient(), helper.testLogger());

      const err = new Error('RetryableException');
      err.retryable = true;

      t.docClient.query
        .onCall(0).yields(err)
        .onCall(1).yields(null, { Items: [{ name: 'Tim Tester', email: 'test@test.com' }] });

      const stream = new Query('tim', t, Serializer).exec();

      let called = false;

      stream.on('readable', () => {
        called = true;

        const data = stream.read();
        if (data) {
          expect(data.Items).to.have.length.above(0);
        }
      });

      stream.on('end', () => {
        expect(called).to.be.true;
        return done();
      });
    });
  });

  describe('#limit', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      table.schema = new Schema(config);
    });

    it('should set the limit', () => {
      const query = new Query('tim', table, serializer).limit(10);
      query.request.Limit.should.equal(10);
    });

    it('should throw when limit is zero', () => {
      const query = new Query('tim', table, serializer);

      expect(() => {
        query.limit(0);
      }).to.throw('Limit must be greater than 0');
    });
  });

  describe('#filterExpression', () => {
    it('should set filter expression', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).filterExpression('Postedby = :val');

      query.request.FilterExpression.should.equal('Postedby = :val');
    });
  });

  describe('#expressionAttributeValues', () => {
    it('should set expression attribute values', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).expressionAttributeValues({ ':val': 'test' });

      query.request.ExpressionAttributeValues.should.eql({ ':val': 'test' });
    });
  });

  describe('#expressionAttributeNames', () => {
    it('should set expression attribute names', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).expressionAttributeNames({ '#name': 'name' });

      query.request.ExpressionAttributeNames.should.eql({ '#name': 'name' });
    });
  });

  describe('#projectionExpression', () => {
    it('should set projection expression', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).projectionExpression('#name, #email');

      query.request.ProjectionExpression.should.eql('#name, #email');
    });
  });

  describe('#usingIndex', () => {
    it('should set the index name to use', () => {
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

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).usingIndex('CreatedIndex');

      query.request.IndexName.should.equal('CreatedIndex');
    });

    it('should create key condition for global index hash key', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        },
        indexes: [{ hashKey: 'age', type: 'global', name: 'UserAgeIndex' }]
      };

      table.schema = new Schema(config);

      serializer.serializeItem.returns({ age: { N: '18' } });

      const query = new Query(18, table, serializer).usingIndex('UserAgeIndex');
      query.exec();

      query.request.IndexName.should.equal('UserAgeIndex');

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      query.request.ExpressionAttributeValues.should.eql({ ':age': 18 });
      query.request.KeyConditionExpression.should.eql('(#age = :age)');
    });
  });

  describe('#consistentRead', () => {
    beforeEach(() => {
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

      table.schema = new Schema(config);
    });

    it('should set Consistent Read to true', () => {
      const query = new Query('tim', table, serializer).consistentRead(true);
      query.request.ConsistentRead.should.be.true;
    });

    it('should set Consistent Read to true when passing no args', () => {
      const query = new Query('tim', table, serializer).consistentRead();
      query.request.ConsistentRead.should.be.true;
    });

    it('should set Consistent Read to false', () => {
      const query = new Query('tim', table, serializer).consistentRead(false);
      query.request.ConsistentRead.should.be.false;
    });
  });

  describe('#attributes', () => {
    beforeEach(() => {
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

      table.schema = new Schema(config);
    });

    it('should set array attributes to get', () => {
      const query = new Query('tim', table, serializer).attributes(['created', 'email']);
      query.request.ProjectionExpression.should.eql('#created,#email');
      query.request.ExpressionAttributeNames.should.eql({ '#created': 'created', '#email': 'email' });
    });

    it('should set single attribute to get', () => {
      const query = new Query('tim', table, serializer).attributes('email');
      query.request.ProjectionExpression.should.eql('#email');
      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
    });
  });

  describe('#order', () => {
    beforeEach(() => {
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

      table.schema = new Schema(config);
    });

    it('should set scan index forward to true', () => {
      const query = new Query('tim', table, serializer).ascending();
      query.request.ScanIndexForward.should.be.true;
    });

    it('should set scan index forward to false', () => {
      const query = new Query('tim', table, serializer).descending();
      query.request.ScanIndexForward.should.be.false;
    });
  });

  describe('#startKey', () => {
    beforeEach(() => {
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

      table.schema = new Schema(config);
    });

    it('should set start Key', () => {
      const key = { name: { S: 'tim' }, email: { S: 'foo@example.com' } };
      serializer.buildKey.returns(key);

      const query = new Query('tim', table, serializer).startKey({ name: 'tim', email: 'foo@example.com' });

      query.request.ExclusiveStartKey.should.eql(key);
    });
  });

  describe('#select', () => {
    it('should set select Key', () => {
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

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).select('COUNT');

      query.request.Select.should.eql('COUNT');
    });
  });

  describe('#ReturnConsumedCapacity', () => {
    beforeEach(() => {
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

      table.schema = new Schema(config);
    });

    it('should set return consumed capacity Key to passed in value', () => {
      const query = new Query('tim', table, serializer).returnConsumedCapacity('TOTAL');

      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });

    it('should set return consumed capacity Key', () => {
      const query = new Query('tim', table, serializer).returnConsumedCapacity();

      query.request.ReturnConsumedCapacity.should.eql('TOTAL');
    });
  });

  describe('#where', () => {
    let query;

    beforeEach(() => {
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

      table.schema = new Schema(config);
      query = new Query('tim', table, serializer);
    });

    it('should have hash key and range key equals clauses', () => {
      query = query.where('email').equals('foo@example.com');
      query.exec();

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email', '#name': 'name' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com', ':name': 'tim' });
      query.request.KeyConditionExpression.should.eql('(#email = :email) AND (#name = :name)');
    });

    it('should have equals clause', () => {
      query = query.where('email').equals('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      query.request.KeyConditionExpression.should.eql('(#email = :email)');
    });

    it('should have less than or equal clause', () => {
      query = query.where('email').lte('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      query.request.KeyConditionExpression.should.eql('(#email <= :email)');
    });

    it('should have less than clause', () => {
      query = query.where('email').lt('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      query.request.KeyConditionExpression.should.eql('(#email < :email)');
    });

    it('should have greater than or equal clause', () => {
      query = query.where('email').gte('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      query.request.KeyConditionExpression.should.eql('(#email >= :email)');
    });

    it('should have greater than clause', () => {
      query = query.where('email').gt('foo@example.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com' });
      query.request.KeyConditionExpression.should.eql('(#email > :email)');
    });

    it('should have begins with clause', () => {
      query = query.where('email').beginsWith('foo');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo' });
      query.request.KeyConditionExpression.should.eql('(begins_with(#email, :email))');
    });

    it('should have between clause', () => {
      query = query.where('email').between('bob@bob.com', 'foo@foo.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'bob@bob.com', ':email_2': 'foo@foo.com' });
      query.request.KeyConditionExpression.should.eql('(#email BETWEEN :email AND :email_2)');
    });

    it('should support multiple clauses on same attribute', () => {
      query = query.where('email').gt('foo@example.com').where('email').lt('moo@foo.com');

      query.request.ExpressionAttributeNames.should.eql({ '#email': 'email' });
      query.request.ExpressionAttributeValues.should.eql({ ':email': 'foo@example.com', ':email_2': 'moo@foo.com' });
      query.request.KeyConditionExpression.should.eql('(#email > :email) AND (#email < :email_2)');
    });
  });

  describe('#filter', () => {
    let query;

    beforeEach(() => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          created: Joi.date(),
          age: Joi.number(),
          data: Joi.object()
        },
        indexes: [{ hashKey: 'name', rangeKey: 'created', type: 'local', name: 'CreatedIndex' }]
      };

      table.schema = new Schema(config);

      query = new Query('tim', table, serializer);
    });

    it('should have equals clause', () => {
      query = query.filter('age').equals(5);

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      query.request.ExpressionAttributeValues.should.eql({ ':age': 5 });
      query.request.FilterExpression.should.eql('(#age = :age)');
    });

    it('should have exists clause', () => {
      query = query.filter('age').exists();

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      expect(query.request.ExpressionAttributeValues).to.not.exist;
      query.request.FilterExpression.should.eql('(attribute_exists(#age))');
    });

    it('should have not exists clause', () => {
      query = query.filter('age').exists(false);

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      expect(query.request.ExpressionAttributeValues).to.not.exist;
      query.request.FilterExpression.should.eql('(attribute_not_exists(#age))');
    });

    it('should have between clause', () => {
      query = query.filter('age').between(5, 7);

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      query.request.ExpressionAttributeValues.should.eql({ ':age': 5, ':age_2': 7 });
      query.request.FilterExpression.should.eql('(#age BETWEEN :age AND :age_2)');
    });

    it('should have IN clause', () => {
      query = query.filter('age').in([5, 7, 12]);

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      query.request.ExpressionAttributeValues.should.eql({ ':age': 5, ':age_2': 7, ':age_3': 12 });
      query.request.FilterExpression.should.eql('(#age IN (:age,:age_2,:age_3))');
    });

    it('should support multiple filters on same attribute', () => {
      query = query.filter('age').gt(5).filter('age').lt(20).filter('age').ne(15);

      query.request.ExpressionAttributeNames.should.eql({ '#age': 'age' });
      query.request.ExpressionAttributeValues.should.eql({ ':age': 5, ':age_2': 20, ':age_3': 15 });
      query.request.FilterExpression.should.eql('(#age > :age) AND (#age < :age_2) AND (#age <> :age_3)');
    });

    it('should support Map.Attr document paths', () => {
      query = query.filter('data.attr').equals(15);

      query.request.ExpressionAttributeNames.should.eql({ '#data': 'data', '#attr': 'attr' });
      query.request.ExpressionAttributeValues.should.eql({ ':data_attr': 15 });
      query.request.FilterExpression.should.eql('(#data.#attr = :data_attr)');
    });
  });

  describe('#loadAll', () => {
    it('should set load all option to true', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const query = new Query('tim', table, serializer).loadAll();

      query.options.loadAll.should.be.true;
    });
  });
});
