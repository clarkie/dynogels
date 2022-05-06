'use strict';

const Item = require('../lib/item');
const Table = require('../lib/table');
const Schema = require('../lib/schema');
const chai = require('chai');
const helper = require('./test-helper');
const serializer = require('../lib/serializer');
const Joi = require('joi');

const expect = chai.expect;

chai.should();

describe('item', () => {
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

    table = new Table('mockTable', schema, serializer, helper.mockDocClient(), helper.testLogger());
  });

  it('JSON.stringify should only serialize attrs', () => {
    const attrs = { num: 1, name: 'foo' };
    const item = new Item(attrs, table);
    const stringified = JSON.stringify(item);

    stringified.should.equal(JSON.stringify(attrs));
  });

  describe('#save', () => {
    it('should return error', (done) => {
      table.docClient.put.yields(new Error('fail'));

      const attrs = { num: 1, name: 'foo' };
      const item = new Item(attrs, table);

      item.save((err, data) => {
        expect(err).to.exist;
        expect(data).to.not.exist;

        return done();
      });
    });
  });

  describe('#update', () => {
    it('should return item', (done) => {
      table.docClient.update.yields(null, { Attributes: { num: 1, name: 'foo' } });

      const attrs = { num: 1, name: 'foo' };
      const item = new Item(attrs, table);

      item.update((err, data) => {
        expect(err).to.not.exist;
        expect(data.get()).to.eql({ num: 1, name: 'foo' });

        return done();
      });
    });


    it('should return error', (done) => {
      table.docClient.update.yields(new Error('fail'));

      const attrs = { num: 1, name: 'foo' };
      const item = new Item(attrs, table);

      item.update((err, data) => {
        expect(err).to.exist;
        expect(data).to.not.exist;

        return done();
      });
    });

    it('should return null', (done) => {
      table.docClient.update.yields(null, {});

      const attrs = { num: 1, name: 'foo' };
      const item = new Item(attrs, table);

      item.update((err, data) => {
        expect(err).to.not.exist;
        expect(data.get()).to.eql({ num: 1 });

        return done();
      });
    });
  });

  describe('#set', () => {
    it('should set attributes', () => {
      const item = new Item({});
      item.set({ num: 1, name: 'foo' });
      expect(item.get('num')).to.equal(1);
      expect(item.get('name')).to.equal('foo');
    });

    it('should set a single key when provided a string and value', () => {
      const item = new Item({});
      item.set('num', 123);
      expect(item.get('num')).to.equal(123);
    });

    it('should set an empty array', () => {
      const item = new Item({ array: [1, 2, 3] });
      item.set({ array: [] });
      const setField = item.get('array');
      expect(setField).to.be.an('array');
      expect(setField).to.have.length(0);
    });

    it('should combine objects in array when setting array field', () => {
      const item = new Item({ a: [{ b: 2 }, { d: 4 }] });
      item.set({ a: [{ c: 3 }, { e: 5 }] });
      const setField = item.get('a');
      expect(setField[0].b).to.equal(2);
      expect(setField[0].c).to.equal(3);
      expect(setField[1].d).to.equal(4);
      expect(setField[1].e).to.equal(5);
    });
  });
});

