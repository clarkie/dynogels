'use strict';

const dynogels = require('../index');
const AWS = require('aws-sdk');
const helper = require('./test-helper');
const Table = require('../lib/table');
const chai = require('chai');
const Joi = require('joi');
const sinon = require('sinon');

const expect = chai.expect;

chai.should();

describe('dynogels', () => {
  afterEach(() => {
    dynogels.reset();
  });

  describe('#define', () => {
    it('should return model', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string()
        }
      };

      const model = dynogels.define('Account', config);
      expect(model).to.be.a('function');
    });

    it('should throw when using old api', () => {
      expect(() => {
        dynogels.define('Account', (schema) => {
          schema.String('email', { hashKey: true });
        });
      }).to.throw(/define no longer accepts schema callback, migrate to new api/);
    });

    it('should have config method', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      Account.config({ tableName: 'test-accounts' });

      Account.config().name.should.equal('test-accounts');
    });

    it('should configure table name as accounts', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      Account.config().name.should.equal('account');
    });

    it('should return new account item', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      const acc = new Account({ name: 'Test Acc' });
      acc.table.should.be.instanceof(Table);
    });

    it('should have validate method', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });
      expect(Account.validate).to.exist;
      expect(Account.validate).to.be.a('function');
    });
  });

  describe('#models', () => {
    it('should be empty', () => {
      dynogels.models.should.be.empty;
    });

    it('should contain single model', () => {
      dynogels.define('Account', { hashKey: 'id' });

      dynogels.models.should.contain.keys('Account');
    });
  });

  describe('#model', () => {
    it('should return defined model', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      dynogels.model('Account').should.equal(Account);
    });

    it('should return null', () => {
      expect(dynogels.model('Person')).to.be.null;
    });
  });

  describe('model config', () => {
    it('should configure set dynamodb driver', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      Account.config({ tableName: 'test-accounts' });

      Account.config().name.should.eq('test-accounts');
    });

    it('should configure set dynamodb driver', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      const dynamodb = helper.realDynamoDB();
      Account.config({ dynamodb: dynamodb });

      Account.docClient.service.config.endpoint.should.eq(dynamodb.config.endpoint);
    });

    it('should set document client', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      const docClient = new AWS.DynamoDB.DocumentClient(helper.realDynamoDB());

      Account.config({ docClient: docClient });

      Account.docClient.should.eq(docClient);
    });


    it('should globally set dynamodb driver for all models', () => {
      const Account = dynogels.define('Account', { hashKey: 'id' });
      const Post = dynogels.define('Post', { hashKey: 'id' });

      const dynamodb = helper.realDynamoDB();
      dynogels.dynamoDriver(dynamodb);

      Account.docClient.service.config.endpoint.should.eq(dynamodb.config.endpoint);
      Post.docClient.service.config.endpoint.should.eq(dynamodb.config.endpoint);
    });

    it('should continue to use globally set dynamodb driver', () => {
      const dynamodb = helper.mockDynamoDB();
      dynogels.dynamoDriver(dynamodb);

      const Account = dynogels.define('Account', { hashKey: 'id' });

      Account.docClient.service.config.endpoint.should.eq(dynamodb.config.endpoint);
    });
  });

  describe('#createTables', () => {
    let clock;

    beforeEach(() => {
      dynogels.reset();
      // var dynamodb = helper.mockDynamoDB();
      // dynogels.dynamoDriver(dynamodb);
      dynogels.documentClient(helper.mockDocClient());
      clock = sinon.useFakeTimers();
    });

    afterEach(() => {
      clock.restore();
    });

    it('should create single definied model', function (done) {
      this.timeout(0);

      const Account = dynogels.define('Account', { hashKey: 'id' });

      const second = {
        Table: { TableStatus: 'PENDING' }
      };

      const third = {
        Table: { TableStatus: 'ACTIVE' }
      };

      const dynamodb = Account.docClient.service;

      dynamodb.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(null, third);

      dynamodb.createTable.yields(null, null);

      dynogels.createTables((err) => {
        expect(err).to.not.exist;
        expect(dynamodb.describeTable.calledThrice).to.be.true;
        return done();
      });

      clock.tick(1200);
      clock.tick(1200);
    });

    it('should return error', (done) => {
      const Account = dynogels.define('Account', { hashKey: 'id' });

      const dynamodb = Account.docClient.service;
      dynamodb.describeTable.onCall(0).yields(null, null);

      dynamodb.createTable.yields(new Error('Fail'), null);

      dynogels.createTables((err) => {
        expect(err).to.exist;
        expect(dynamodb.describeTable.calledOnce).to.be.true;
        return done();
      });
    });

    it('should create model without callback', (done) => {
      const Account = dynogels.define('Account', { hashKey: 'id' });
      const dynamodb = Account.docClient.service;

      const second = {
        Table: { TableStatus: 'PENDING' }
      };

      const third = {
        Table: { TableStatus: 'ACTIVE' }
      };

      dynamodb.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(null, third);

      dynamodb.createTable.yields(null, null);

      dynogels.createTables();

      clock.tick(1200);
      clock.tick(1200);

      expect(dynamodb.describeTable.calledThrice).to.be.true;
      return done();
    });

    it('should return error when waiting for table to become active', (done) => {
      const Account = dynogels.define('Account', { hashKey: 'id' });
      const dynamodb = Account.docClient.service;

      const second = {
        Table: { TableStatus: 'PENDING' }
      };

      dynamodb.describeTable
        .onCall(0).yields(null, null)
        .onCall(1).yields(null, second)
        .onCall(2).yields(new Error('fail'));

      dynamodb.createTable.yields(null, null);

      dynogels.createTables((err) => {
        expect(err).to.exist;
        expect(dynamodb.describeTable.calledThrice).to.be.true;
        return done();
      });

      clock.tick(1200);
      clock.tick(1200);
    });
  });
});
