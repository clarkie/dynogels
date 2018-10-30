'use strict';

const helper = require('./test-helper');
const chai = require('chai');
const Schema = require('../lib/schema');
const Item = require('../lib/item');
const batch = require('../lib/batch');
const Serializer = require('../lib/serializer');
const Joi = require('joi');
const _ = require('lodash');

const expect = chai.expect;

describe('Batch', () => {
  let serializer;
  let table;

  beforeEach(() => {
    serializer = helper.mockSerializer();

    table = helper.mockTable();
    table.serializer = Serializer;
    table.tableName = () => 'accounts';

    const config = {
      hashKey: 'name',
      rangeKey: 'email',
      schema: {
        name: Joi.string(),
        email: Joi.string(),
        age: Joi.number()
      }
    };

    table.schema = new Schema(config);
  });

  describe('#getItems', () => {
    it('should get items by hash key', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const response = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' },
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        }
      };

      const expectedRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: 'test@test.com' },
              { email: 'foo@example.com' }
            ]
          }
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester' };
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], (err, items) => {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items by hash and range key', (done) => {
      const key1 = { email: 'test@test.com', name: 'Tim Tester' };
      const key2 = { email: 'foo@example.com', name: 'Foo Bar' };

      const response = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' },
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        }
      };

      const expectedRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: key1.email, name: key1.name },
              { email: key2.email, name: key2.name }
            ]
          }
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester', age: 22 };
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems([key1, key2], (err, items) => {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should not modify passed in keys', (done) => {
      const keys = _.map(_.range(100), (num) => {
        const key = { email: `test${num}@test.com`, name: `Test ${num}` };
        serializer.buildKey.withArgs(key).returns({ email: { S: key.email }, name: { S: key.name } });

        return key;
      });

      const item1 = { email: 'test@test.com', name: 'Tim Tester', age: 22 };
      table.runBatchGetItems.yields(null, {});
      serializer.deserializeItem.returns(item1);

      table.initItem.returns(new Item(item1));

      batch(table, serializer).getItems(keys, () => {
        _.each(_.range(100), (num) => {
          const key = { email: `test${num}@test.com`, name: `Test ${num}` };
          keys[num].should.eql(key);
        });

        done();
      });
    });

    it('should get items by hash key with consistent read', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const response = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' },
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        }
      };

      const expectedRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: 'test@test.com' },
              { email: 'foo@example.com' }
            ],
            ConsistentRead: true
          }
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester' };
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], { ConsistentRead: true }, (err, items) => {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items by hash key with projection expression', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const response = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' },
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        }
      };

      const expectedRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: 'test@test.com' },
              { email: 'foo@example.com' }
            ],
            ProjectionExpression: '#name, #e',
            ExpressionAttributeNames: { '#name': 'name', '#email': 'email' }
          }
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester' };
      table.runBatchGetItems.withArgs(expectedRequest).yields(null, response);

      table.initItem.returns(new Item(item1));

      const opts = {
        ProjectionExpression: '#name, #e',
        ExpressionAttributeNames: { '#name': 'name', '#email': 'email' }
      };

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], opts, (err, items) => {
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });

    it('should get items when encounters retryable excpetion', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const response = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' },
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester' };

      const err = new Error('RetryableException');
      err.retryable = true;

      table.runBatchGetItems
        .onCall(0).yields(err)
        .onCall(1).yields(null, response);

      table.initItem.returns(new Item(item1));

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], (err, items) => {
        expect(err).to.not.exist;

        expect(table.runBatchGetItems.calledTwice).to.be.true;
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');

        done();
      });
    });


    it('should get unprocessed keys', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const firstResponse = {
        Responses: {
          accounts: [
            { email: 'test@test.com', name: 'Tim Tester' }
          ]
        },
        UnprocessedKeys: {
          accounts: {
            Keys: [
              { email: 'foo@example.com' }
            ]
          }
        }
      };

      const secondResponse = {
        Responses: {
          accounts: [
            { email: 'foo@example.com', name: 'Foo Bar' }
          ]
        },
      };

      const expectedFirstRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: 'test@test.com' },
              { email: 'foo@example.com' },
            ]
          }
        }
      };

      const expectedSecondRequest = {
        RequestItems: {
          accounts: {
            Keys: [
              { email: 'foo@example.com' }
            ],
          }
        }
      };

      const item1 = { email: 'test@test.com', name: 'Tim Tester' };
      const item2 = { email: 'foo@example.com', name: 'Foo Bar' };
      table.initItem.onCall(0).returns(new Item(item1));
      table.initItem.onCall(1).returns(new Item(item2));

      table.runBatchGetItems
        .withArgs(expectedFirstRequest).yields(null, firstResponse)
        .withArgs(expectedSecondRequest).yields(null, secondResponse);

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], (err, items) => {
        expect(err).to.not.exist;

        expect(table.runBatchGetItems.calledTwice).to.be.true;
        items.should.have.length(2);
        items[0].get('email').should.equal('test@test.com');
        items[1].get('email').should.equal('foo@example.com');
        done();
      });
    });

    it('should return error', (done) => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      table.schema = new Schema(config);

      const err = new Error('Error');
      table.runBatchGetItems.onCall(0).yields(err);

      batch(table, Serializer).getItems(['test@test.com', 'foo@example.com'], (err, items) => {
        expect(err).to.exist;
        expect(items).to.not.exist;

        expect(table.runBatchGetItems.calledOnce).to.be.true;
        done();
      });
    });
  });
});
