'use strict';

const helper = require('./test-helper');
const _ = require('lodash');
const Joi = require('joi');
const Table = require('../lib/table');
const Schema = require('../lib/schema');
const Query = require('../lib//query');
const Scan = require('../lib//scan');
const Item = require('../lib/item');
const realSerializer = require('../lib/serializer');
const chai = require('chai');
const sinon = require('sinon');

const expect = chai.expect;

chai.should();

describe('table', () => {
  let table;
  let serializer;
  let docClient;
  let dynamodb;
  let logger;

  beforeEach(() => {
    serializer = helper.mockSerializer();
    docClient = helper.mockDocClient();
    dynamodb = docClient.service;
    logger = helper.testLogger();
  });

  describe('#get', () => {
    it('should get item by hash key', (done) => {
      const config = {
        hashKey: 'email'
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', (err, account) => {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hash and range key', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email'
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          name: 'Tim Tester',
          email: 'test@test.com'
        }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'Tim Tester' }
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('Tim Tester', 'test@test.com', (err, account) => {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item by hash key and options', (done) => {
      const config = {
        hashKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ConsistentRead: true
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', { ConsistentRead: true }, (err, account) => {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should get item by hashkey, range key and options', (done) => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          name: 'Tim Tester',
          email: 'test@test.com'
        },
        ConsistentRead: true
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'Tim Tester' }
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('Tim Tester', 'test@test.com', { ConsistentRead: true }, (err, account) => {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Tester');

        done();
      });
    });

    it('should get item from dynamic table by hash key', (done) => {
      const config = {
        hashKey: 'email',
        tableName: function () {
          return 'accounts_2014';
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts_2014',
        Key: { email: 'test@test.com' }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).yields(null, resp);

      table.get('test@test.com', (err, account) => {
        account.should.be.instanceof(Item);
        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('test dude');

        done();
      });
    });

    it('should return error', (done) => {
      const config = {
        hashKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.get.yields(new Error('Fail'));

      table.get('test@test.com', (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });
  });

  describe('#create', () => {
    it('should create valid item', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Tim Test',
          age: 23
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create(request.Item, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');

        done();
      });
    });

    it('should call apply defaults', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string().default('Foo'),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Foo',
          age: 23
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com', age: 23 }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Foo');

        done();
      });
    });

    it('should omit null values', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number().allow(null),
          favoriteNumbers: Schema.types.numberSet().allow(null),
          luckyNumbers: Schema.types.numberSet().allow(null)
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const numberSet = sinon.match((value) => {
        const s = docClient.createSet([1, 2, 3]);

        value.type.should.eql('Number');
        value.values.should.eql(s.values);

        return true;
      }, 'NumberSet');

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Tim Test',
          luckyNumbers: numberSet
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      const item = { email: 'test@test.com', name: 'Tim Test', age: null, favoriteNumbers: [], luckyNumbers: [1, 2, 3] };
      table.create(item, (err, account) => {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('luckyNumbers').should.eql([1, 2, 3]);

        expect(account.toJSON()).to.have.keys(['email', 'name', 'luckyNumbers']);

        done();
      });
    });

    it('should omit empty values', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string().allow(''),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          age: 2
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com', name: '', age: 2 }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('age').should.equal(2);

        done();
      });
    });

    it('should create item with createdAt timestamp', (done) => {
      const config = {
        hashKey: 'email',
        timestamps: true,
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          createdAt: sinon.match.string
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com' }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('createdAt').should.exist;
        done();
      });
    });

    it('should create item with custom createdAt attribute name', (done) => {
      const config = {
        hashKey: 'email',
        timestamps: true,
        createdAt: 'created',
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          created: sinon.match.string
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com' }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('created').should.exist;
        done();
      });
    });


    it('should create item without createdAt param', (done) => {
      const config = {
        hashKey: 'email',
        timestamps: true,
        createdAt: false,
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com'
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com' }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        expect(account.get('createdAt')).to.not.exist;
        done();
      });
    });

    it('should create item with expected option', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression: '(#name = :name)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com' }, { expected: { name: 'Foo Bar' } }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with no callback', (done) => {
      const config = {
        hashKey: 'email',
        timestamps: true,
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com' });

      docClient.put.calledWith(request);
      return done();
    });

    it('should return validation error', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      table.create({ email: 'test@test.com', name: [1, 2, 3] }, (err, account) => {
        expect(err).to.exist;
        expect(err).to.match(/ValidationError/);
        expect(account).to.not.exist;

        sinon.assert.notCalled(docClient.put);
        done();
      });
    });

    it('should fail with custom errors specified in schema', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string().email({ errorLevel: true }).required(),
          custom: Joi.any().forbidden().error(new Error('Only hashed passwords should be persisted')),
        }
      };

      const s = new Schema(config);

      const table = new Table('accounts', s, realSerializer, docClient, logger);

      table.create({ email: 'test@test.com', custom: 'forbidden text' }, (err, account) => {
        expect(err).to.exist;
        expect(err).to.match(/hashed passwords/);
        expect(account).to.not.exist;

        sinon.assert.notCalled(docClient.put);
        done();
      });
    });

    it('should create item with condition expression on hashkey when overwrite flag is false', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email' },
        ExpressionAttributeValues: { ':email': 'test@test.com' },
        ConditionExpression: '(#email <> :email)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: false }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item with condition expression on hash and range key when overwrite flag is false', (done) => {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email', '#name': 'name' },
        ExpressionAttributeValues: { ':email': 'test@test.com', ':name': 'Bob Tester' },
        ConditionExpression: '(#email <> :email) AND (#name <> :name)'
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: false }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });

    it('should create item without condition expression when overwrite flag is true', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        }
      };

      docClient.put.withArgs(request).yields(null, {});

      table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: true }, (err, account) => {
        expect(err).to.not.exist;
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        done();
      });
    });
  });

  describe('#update', () => {
    it('should update valid item', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      docClient.update.withArgs(request).yields(null, { Attributes: returnedAttributes });

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
      table.update(item, (err, account) => {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        done();
      });
    });

    it('should accept falsy key and range values', (done) => {
      const config = {
        hashKey: 'userId',
        rangeKey: 'timeOffset',
        schema: {
          userId: Joi.number(),
          timeOffset: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('users', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'users',
        Key: { userId: 0, timeOffset: 0 },
        ReturnValues: 'ALL_NEW'
      };

      const returnedAttributes = { userId: 0, timeOffset: 0 };

      docClient.update.withArgs(request).yields(null, { Attributes: returnedAttributes });

      const item = { userId: 0, timeOffset: 0 };
      table.update(item, (err, user) => {
        expect(err).to.be.null;

        user.should.be.instanceof(Item);

        user.get('userId').should.equal(0);
        user.get('timeOffset').should.equal(0);

        done();
      });
    });

    it('should update with passed in options', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_OLD',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name_2': 'Foo Bar', ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' },
        ConditionExpression: '(#name = :name_2)'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      docClient.update.withArgs(request).yields(null, { Attributes: returnedAttributes });

      const getOptions = function () {
        return { ReturnValues: 'ALL_OLD', expected: { name: 'Foo Bar' } };
      };

      const passedOptions = getOptions();

      table.update(item, passedOptions, (err, account) => {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);

        expect(passedOptions).to.deep.equal(getOptions());

        done();
      });
    });

    it('should update merge update expressions when passed in as options', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age ADD #color :c',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23, ':c': 'red' },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age', '#color': 'color' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86],
        color: 'red'
      };

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      docClient.update.withArgs(request).yields(null, { Attributes: returnedAttributes });

      const options = {
        UpdateExpression: 'ADD #color :c',
        ExpressionAttributeValues: { ':c': 'red' },
        ExpressionAttributeNames: { '#color': 'color' }
      };

      table.update(item, options, (err, account) => {
        account.should.be.instanceof(Item);

        account.get('email').should.equal('test@test.com');
        account.get('name').should.equal('Tim Test');
        account.get('age').should.equal(23);
        account.get('scores').should.eql([97, 86]);
        account.get('color').should.eql('red');

        done();
      });
    });

    it('should update valid item without a callback', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      docClient.update.withArgs(request).yields(null, { Attributes: returnedAttributes });

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
      table.update(item);

      docClient.update.calledWith(request);
      return done();
    });

    it('should return error', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.update.yields(new Error('Fail'));

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      table.update(item, (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

    it('should handle errors regarding invalid expressions', (done) => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
          birthday: Joi.date().iso()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const item = { name: 'Dr. Who', birthday: undefined };

      table.update(item, (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });
  });

  describe('#query', () => {
    it('should return query object', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.query('Bob').should.be.instanceof(Query);
    });
  });

  describe('#scan', () => {
    it('should return scan object', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.scan().should.be.instanceof(Scan);
    });
  });

  describe('#destroy', () => {
    it('should destroy valid item', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        }
      };

      docClient.delete.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', () => {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should destroy valid item with falsy hash and range keys', (done) => {
      const config = {
        hashKey: 'userId',
        rangeKey: 'timeOffset',
        schema: {
          hashKey: Joi.number(),
          rangeKey: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('users', s, serializer, docClient, logger);

      const request = {
        TableName: 'users',
        Key: {
          userId: 0,
          timeOffset: 0
        }
      };

      docClient.delete.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy({ userId: 0, timeOffset: 0 }, () => {
        serializer.buildKey.calledWith(0, 0, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should take optional params', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: { S: 'test@test.com' }
        },
        ReturnValues: 'ALL_OLD'
      };

      docClient.delete.yields(null, {});

      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', { ReturnValues: 'ALL_OLD' }, () => {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should parse and return attributes', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_OLD'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.yields(null, { Attributes: returnedAttributes });

      serializer.buildKey.returns(request.Key);
      serializer
        .deserializeItem
        .withArgs(returnedAttributes)
        .returns({ email: 'test@test.com', name: 'Foo Bar' });

      table.destroy('test@test.com', { ReturnValues: 'ALL_OLD' }, (err, item) => {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hash and range key', (done) => {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com',
          name: 'Foo Bar'
        }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.yields(null, { Attributes: returnedAttributes });

      serializer.buildKey.returns(request.Key);
      serializer
        .deserializeItem
        .withArgs(returnedAttributes)
        .returns({ email: 'test@test.com', name: 'Foo Bar' });

      table.destroy('test@test.com', 'Foo Bar', (err, item) => {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should accept hashkey rangekey and options', (done) => {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com',
          name: 'Foo Bar'
        },
        ReturnValues: 'ALL_OLD'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.yields(null, { Attributes: returnedAttributes });

      serializer.buildKey.returns(request.Key);
      serializer
        .deserializeItem
        .withArgs(returnedAttributes)
        .returns({ email: 'test@test.com', name: 'Foo Bar' });

      table.destroy('test@test.com', 'Foo Bar', { ReturnValues: 'ALL_OLD' }, (err, item) => {
        serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        item.get('name').should.equal('Foo Bar');

        done();
      });
    });

    it('should serialize expected option', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression: '(#name = :name)'
      };

      docClient.delete.yields(null, {});

      serializer.serializeItem.withArgs(s, { name: 'Foo Bar' }, { expected: true }).returns(request.Expected);
      serializer.buildKey.returns(request.Key);

      table.destroy('test@test.com', { expected: { name: 'Foo Bar' } }, () => {
        serializer.buildKey.calledWith('test@test.com', null, s).should.be.true;
        docClient.delete.calledWith(request).should.be.true;

        done();
      });
    });

    it('should call delete item without callback', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        }
      };

      docClient.delete.yields(null, {});
      table.destroy('test@test.com');

      docClient.delete.calledWith(request);

      return done();
    });

    it('should call delete item with hash key, options and no callback', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        },
        Expected: {
          name: { Value: 'Foo Bar' }
        }
      };

      docClient.delete.yields(null, {});
      table.destroy('test@test.com', { expected: { name: 'Foo Bar' } });

      docClient.delete.calledWith(request);

      return done();
    });
  });

  describe('#describeTable', () => {
    it('should make describe table request', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts'
      };

      dynamodb.describeTable.yields(null, {});

      table.describeTable((err) => {
        expect(err).to.be.null;
        dynamodb.describeTable.calledWith(request).should.be.true;
        done();
      });
    });
  });

  describe('#updateTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make update table request', (done) => {
      const request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 4, WriteCapacityUnits: 2 }
      };

      dynamodb.describeTable.yields(null, {});
      dynamodb.updateTable.yields(null, {});

      table.updateTable({ readCapacity: 4, writeCapacity: 2 }, (err) => {
        expect(err).to.be.null;
        dynamodb.updateTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make update table request without callback', (done) => {
      const request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 2, WriteCapacityUnits: 1 }
      };

      table.updateTable({ readCapacity: 2, writeCapacity: 1 });

      dynamodb.updateTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#deleteTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make delete table request', (done) => {
      const request = {
        TableName: 'accounts'
      };

      dynamodb.deleteTable.yields(null, {});

      table.deleteTable((err) => {
        expect(err).to.be.null;
        dynamodb.deleteTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make delete table request without callback', (done) => {
      const request = {
        TableName: 'accounts',
      };

      table.deleteTable();

      dynamodb.deleteTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#tableName', () => {
    it('should return given name', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts');
    });

    it('should return table name set on schema', () => {
      const config = {
        hashKey: 'email',
        tableName: 'accounts-2014-03',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts-2014-03');
    });

    it('should return table name returned from function on schema', () => {
      const d = new Date();
      const dateString = [d.getFullYear(), d.getMonth() + 1].join('_');

      const nameFunc = () => `accounts_${dateString}`;

      const config = {
        hashKey: 'email',
        tableName: nameFunc,
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql(`accounts_${dateString}`);
    });
  });


  describe('#dynamoCreateTableParams', () => {
    it('should make table arguments with hash key', () => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
      expect(table.dynamoCreateTableParams({ readCapacity: 5, writeCapacity: 5 })).to.deep.equal({
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'email', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      });
    });

    it('should make table arguments with range key', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
      expect(table.dynamoCreateTableParams({ readCapacity: 5, writeCapacity: 5 })).to.deep.equal({
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      });
    });

    it('should make table arguments with stream specification', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
      expect(table.dynamoCreateTableParams({
        readCapacity: 5,
        writeCapacity: 5,
        streamSpecification: {
          streamEnabled: true,
          streamViewType: 'NEW_IMAGE'
        }
      })).to.deep.equal({
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        StreamSpecification: { StreamEnabled: true, StreamViewType: 'NEW_IMAGE' }
      });
    });

    it('should make table arguments with secondary index', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        indexes: [
          { hashKey: 'name', rangeKey: 'age', name: 'ageIndex', type: 'local' }
        ],
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      expect(table.dynamoCreateTableParams({ readCapacity: 5, writeCapacity: 5 })).to.deep.equal({
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' },
          { AttributeName: 'age', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        LocalSecondaryIndexes: [
          {
            IndexName: 'ageIndex',
            KeySchema: [
              { AttributeName: 'name', KeyType: 'HASH' },
              { AttributeName: 'age', KeyType: 'RANGE' }
            ],
            Projection: {
              ProjectionType: 'ALL'
            }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      });
    });

    it('should make table arguments with global secondary index', () => {
      const config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes: [
          { hashKey: 'gameTitle', rangeKey: 'topScore', name: 'GameTitleIndex', type: 'global' }
        ],
        schema: {
          userId: Joi.string(),
          gameTitle: Joi.string(),
          topScore: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);
      expect(table.dynamoCreateTableParams({ readCapacity: 5, writeCapacity: 5 })).to.deep.equal({
        TableName: 'gameScores',
        AttributeDefinitions: [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              ProjectionType: 'ALL'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      });
    });

    it('should make table arguments with global secondary index', () => {
      const config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes: [{
          hashKey: 'gameTitle',
          rangeKey: 'topScore',
          name: 'GameTitleIndex',
          type: 'global',
          readCapacity: 10,
          writeCapacity: 5,
          projection: { NonKeyAttributes: ['wins'], ProjectionType: 'INCLUDE' }
        }],
        schema: {
          userId: Joi.string(),
          gameTitle: Joi.string(),
          topScore: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);
      expect(table.dynamoCreateTableParams({ readCapacity: 5, writeCapacity: 5 })).to.deep.equal({
        TableName: 'gameScores',
        AttributeDefinitions: [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              NonKeyAttributes: ['wins'],
              ProjectionType: 'INCLUDE'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 5 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      });
    });
  });

  describe('#createTable', () => {
    it('should call dynamo.createTable with the dynamoCreateTableParams result', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };
      const s = new Schema(config);
      table = new Table('accounts', s, serializer, docClient, logger);

      const mockCreateTableParamsResult = 'mockResult';

      const options = { readCapacity: 5, writeCapacity: 5 };

      const sandbox = sinon.sandbox.create();
      const dynamoCreateTableParamsStub = sandbox.stub(Table.prototype, 'dynamoCreateTableParams');
      dynamoCreateTableParamsStub.callsFake(() => mockCreateTableParamsResult);

      dynamodb.createTable.yields(null, {});

      table.createTable(options, (err) => {
        expect(err).to.be.null;
        dynamoCreateTableParamsStub.calledOnce.should.be.true;
        expect(dynamoCreateTableParamsStub.args[0]).to.deep.equal([options]);
        dynamodb.createTable.calledWith(mockCreateTableParamsResult).should.be.true;
        sandbox.verify();
        sandbox.reset();
        done();
      });
    });
  });

  describe('#describeTable', () => {
    it('should make describe table request', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts'
      };

      dynamodb.describeTable.yields(null, {});

      table.describeTable((err) => {
        expect(err).to.be.null;
        dynamodb.describeTable.calledWith(request).should.be.true;
        done();
      });
    });
  });

  describe('#updateTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make update table request', (done) => {
      const request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 4, WriteCapacityUnits: 2 }
      };

      dynamodb.describeTable.yields(null, {});
      dynamodb.updateTable.yields(null, {});

      table.updateTable({ readCapacity: 4, writeCapacity: 2 }, (err) => {
        expect(err).to.be.null;
        dynamodb.updateTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make update table request without callback', (done) => {
      const request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 2, WriteCapacityUnits: 1 }
      };

      table.updateTable({ readCapacity: 2, writeCapacity: 1 });

      dynamodb.updateTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#deleteTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make delete table request', (done) => {
      const request = {
        TableName: 'accounts'
      };

      dynamodb.deleteTable.yields(null, {});

      table.deleteTable((err) => {
        expect(err).to.be.null;
        dynamodb.deleteTable.calledWith(request).should.be.true;
        done();
      });
    });

    it('should make delete table request without callback', (done) => {
      const request = {
        TableName: 'accounts',
      };

      table.deleteTable();

      dynamodb.deleteTable.calledWith(request).should.be.true;

      return done();
    });
  });

  describe('#tableName', () => {
    it('should return given name', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts');
    });

    it('should return table name set on schema', () => {
      const config = {
        hashKey: 'email',
        tableName: 'accounts-2014-03',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts-2014-03');
    });

    it('should return table name returned from function on schema', () => {
      const d = new Date();
      const dateString = [d.getFullYear(), d.getMonth() + 1].join('_');

      const nameFunc = () => `accounts_${dateString}`;

      const config = {
        hashKey: 'email',
        tableName: nameFunc,
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql(`accounts_${dateString}`);
    });
  });

  describe('hooks', () => {
    describe('#create', () => {
      it('should call before hooks', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.put.yields(null, {});

        serializer.serializeItem.withArgs(s, { email: 'test@test.com', name: 'Tommy', age: 23 }).returns({});

        table.before('create', (data, next) => {
          expect(data).to.exist;
          data.name = 'Tommy';

          return next(null, data);
        });

        table.before('create', (data, next) => {
          expect(data).to.exist;
          data.age = '25';

          return next(null, data);
        });

        table.create(item, (err, item) => {
          expect(err).to.not.exist;
          item.get('name').should.equal('Tommy');
          item.get('age').should.equal('25');

          return done();
        });
      });

      it('should return error when before hook returns error', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('create', (data, next) => next(new Error('fail')));

        table.create({ email: 'foo@bar.com' }, (err, item) => {
          expect(err).to.exist;
          expect(item).to.not.exist;

          return done();
        });
      });

      it('should call after hook', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.put.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        table.after('create', (data) => {
          expect(data).to.exist;

          return done();
        });

        table.create(item, () => {});
      });
    });

    describe('#update', () => {
      it('should call before hook', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.update.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({ email: { S: 'test@test.com' } });
        const modified = { email: 'test@test.com', name: 'Tim Test', age: 44 };
        serializer.serializeItemForUpdate.withArgs(s, 'PUT', modified).returns({});

        serializer.deserializeItem.returns(modified);
        docClient.update.yields(null, {});

        let called = false;
        table.before('update', (data, next) => {
          const attrs = _.merge({}, data, { age: 44 });
          called = true;
          return next(null, attrs);
        });

        table.after('update', () => {
          expect(called).to.be.true;
          return done();
        });

        table.update(item, () => {});
      });

      it('should return error when before hook returns error', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('update', (data, next) => next(new Error('fail')));

        table.update({}, (err) => {
          expect(err).to.exist;
          err.message.should.equal('fail');

          return done();
        });
      });

      it('should call after hook', (done) => {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.update.yields(null, {});

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({ email: { S: 'test@test.com' } });
        serializer.serializeItemForUpdate.returns({});

        serializer.deserializeItem.returns(item);
        docClient.update.yields(null, {});

        table.after('update', () => done());

        table.update(item, () => {});
      });
    });

    it('#destroy should call after hook', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      docClient.delete.yields(null, {});
      serializer.buildKey.returns({});

      table.after('destroy', () => done());

      table.destroy('test@test.com', () => { });
    });
  });

  describe('#validate', () => {
    const helper = (config, attrs) => {
      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
      const item = new Item(attrs, table);
      return table.validate(item);
    };

    it('should succeed for empty item when nothing is required', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string()
        }
      };
      const attrs = {};

      const result = helper(config, attrs);
      expect(result.error).to.be.null;
    });

    it('should succeed with required items', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string().required()
        }
      };
      const attrs = { email: 'test@email.com' };

      const result = helper(config, attrs);
      expect(result.error).to.be.null;
    });

    it('should fail when missing required string field', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string().required()
        }
      };
      const attrs = {};

      const result = helper(config, attrs);
      expect(result.error).to.not.be.null;
      expect(result.error.name).to.eql('ValidationError');
    });

    it('should fail when giving numbers to string types', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string()
        }
      };
      const attrs = { email: 123 };

      const result = helper(config, attrs);
      expect(result.error).to.not.be.null;
      expect(result.error.name).to.eql('ValidationError');
    });

    it('should fail when giving strings to number types', () => {
      const config = {
        hashKey: 'phone',
        schema: {
          email: Joi.number()
        }
      };
      const attrs = { phone: 123 };

      const result = helper(config, attrs);
      expect(result.error).to.not.be.null;
      expect(result.error.name).to.eql('ValidationError');
    });
  });
});
