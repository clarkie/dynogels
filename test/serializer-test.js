'use strict';

const serializer = require('../lib/serializer');
const chai = require('chai');
const Schema = require('../lib/schema');
const helper = require('./test-helper');
const Joi = require('joi');

const expect = chai.expect;

chai.should();

describe('Serializer', () => {
  const docClient = helper.mockDocClient();

  describe('#buildKeys', () => {
    it('should handle string hash key', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      const keys = serializer.buildKey('test@test.com', null, s);

      keys.should.eql({ email: 'test@test.com' });
    });

    it('should handle number hash key', () => {
      const config = {
        hashKey: 'year',
        schema: {
          year: Joi.number()
        }
      };

      const s = new Schema(config);

      const keys = serializer.buildKey(1999, null, s);

      keys.should.eql({ year: 1999 });
    });

    it('should handle date hash key', () => {
      const config = {
        hashKey: 'timestamp',
        schema: {
          timestamp: Joi.date()
        }
      };

      const s = new Schema(config);

      const d = new Date();
      const keys = serializer.buildKey(d, null, s);

      keys.should.eql({ timestamp: d.toISOString() });
    });

    it('should handle string hash and range key', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          slug: Joi.string(),
        }
      };

      const s = new Schema(config);

      const keys = serializer.buildKey('Tim Tester', 'test@test.com', s);

      keys.should.eql({ name: 'Tim Tester', email: 'test@test.com' });
    });

    it('should handle number hash and range key', () => {
      const config = {
        hashKey: 'year',
        rangeKey: 'num',
        schema: {
          year: Joi.number(),
          num: Joi.number(),
        }
      };

      const s = new Schema(config);

      const keys = serializer.buildKey(1988, 1.4, s);

      keys.should.eql({ year: 1988, num: 1.4 });
    });

    it('should handle object containing the hash key', () => {
      const config = {
        hashKey: 'year',
        rangeKey: 'name',
        schema: {
          year: Joi.number(),
          name: Joi.string(),
          slug: Joi.string(),
        }
      };

      const s = new Schema(config);

      const keys = serializer.buildKey({ year: 1988, name: 'Joe' }, null, s);

      keys.should.eql({ year: 1988, name: 'Joe' });
    });

    it('should handle local secondary index keys', () => {
      const config = {
        hashKey: 'email',
        rangeKey: 'age',
        schema: {
          email: Joi.string(),
          age: Joi.number(),
          name: Joi.string(),
        },
        indexes: [{
          hashKey: 'email', rangeKey: 'name', type: 'local', name: 'NameIndex'
        }]
      };

      const s = new Schema(config);

      const data = { email: 'test@example.com', age: 22, name: 'Foo Bar' };
      const keys = serializer.buildKey(data, null, s);

      keys.should.eql({ email: 'test@example.com', age: 22, name: 'Foo Bar' });
    });

    it('should handle global secondary index keys', () => {
      const config = {
        hashKey: 'email',
        rangeKey: 'age',
        schema: {
          email: Joi.string(),
          age: Joi.number(),
          name: Joi.string(),
        },
        indexes: [{
          hashKey: 'age', rangeKey: 'name', type: 'global', name: 'AgeNameIndex'
        }]
      };

      const s = new Schema(config);

      const data = { email: 'test@example.com', age: 22, name: 'Foo Bar' };
      const keys = serializer.buildKey(data, null, s);

      keys.should.eql({ email: 'test@example.com', age: 22, name: 'Foo Bar' });
    });

    it('should handle boolean global secondary index key', () => {
      const config = {
        hashKey: 'email',
        rangeKey: 'age',
        schema: {
          email: Joi.string(),
          age: Joi.number(),
          name: Joi.string(),
          adult: Joi.boolean(),
        },
        indexes: [{
          hashKey: 'adult', rangeKey: 'email', type: 'global', name: 'AdultEmailIndex'
        }]
      };

      const s = new Schema(config);

      const data = { email: 'test@example.com', adult: false };
      const keys = serializer.buildKey(data, null, s);

      keys.should.eql({ email: 'test@example.com', adult: false });
    });
  });

  describe('#serializeItem', () => {
    it('should serialize string attribute', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { name: 'Tim Tester' });

      item.should.eql({ name: 'Tim Tester' });
    });

    it('should serialize number attribute', () => {
      const config = {
        hashKey: 'age',
        schema: {
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { age: 21 });

      item.should.eql({ age: 21 });
    });

    it('should serialize binary attribute', () => {
      const config = {
        hashKey: 'data',
        schema: {
          data: Joi.binary(),
          bin: Joi.binary()
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { data: 'hello', bin: Buffer.from('binary') });

      item.should.eql({ data: Buffer.from('hello'), bin: Buffer.from('binary') });
    });

    it('should serialize number attribute with value zero', () => {
      const config = {
        hashKey: 'age',
        schema: {
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { age: 0 });

      item.should.eql({ age: 0 });
    });


    it('should serialize boolean attribute', () => {
      const config = {
        hashKey: 'agree',
        schema: {
          agree: Joi.boolean(),
        }
      };

      const s = new Schema(config);

      serializer.serializeItem(s, { agree: true }).should.eql({ agree: true });
      serializer.serializeItem(s, { agree: 'true' }).should.eql({ agree: true });

      serializer.serializeItem(s, { agree: false }).should.eql({ agree: false });
      serializer.serializeItem(s, { agree: 'false' }).should.eql({ agree: false });

      serializer.serializeItem(s, { agree: 0 }).should.eql({ agree: false });
    });

    it('should serialize date attribute', () => {
      const config = {
        hashKey: 'time',
        schema: {
          time: Joi.date(),
        }
      };

      const s = new Schema(config);

      const d = new Date();
      const item = serializer.serializeItem(s, { time: d });
      item.should.eql({ time: d.toISOString() });

      const now = Date.now();
      const item2 = serializer.serializeItem(s, { time: now });
      item2.should.eql({ time: new Date(now).toISOString() });
    });

    it('should serialize string set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          names: Schema.types.stringSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { names: ['Tim', 'Steve', 'Bob'] });

      const stringSet = docClient.createSet(['Tim', 'Steve', 'Bob']);

      item.names.type.should.eql('String');
      item.names.values.should.eql(stringSet.values);
    });

    it('should serialize single string set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          names: Schema.types.stringSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { names: 'Tim' });

      const stringSet = docClient.createSet(['Tim']);
      item.names.type.should.eql('String');
      item.names.values.should.eql(stringSet.values);
    });

    it('should number set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          scores: Schema.types.numberSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { scores: [2, 4, 6, 8] });

      const numberSet = docClient.createSet([2, 4, 6, 8]);
      item.scores.type.should.eql('Number');
      item.scores.values.should.eql(numberSet.values);
    });

    it('should single number set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          scores: Schema.types.numberSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { scores: 2 });

      const numberSet = docClient.createSet([2]);
      item.scores.type.should.eql('Number');
      item.scores.values.should.eql(numberSet.values);
    });

    it('should serialize binary set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          data: Schema.types.binarySet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { data: ['hello', 'world'] });

      const binarySet = docClient.createSet([Buffer.from('hello'), Buffer.from('world')]);
      item.data.type.should.eql('Binary');
      item.data.values.should.eql(binarySet.values);
    });

    it('should serialize single binary set attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          data: Schema.types.binarySet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { data: 'hello' });

      const binarySet = docClient.createSet([Buffer.from('hello')]);
      item.data.type.should.eql('Binary');
      item.data.values.should.eql(binarySet.values);
    });

    it('should serialize uuid attribute', () => {
      const config = {
        hashKey: 'id',
        schema: {
          id: Schema.types.uuid(),
        }
      };

      const s = new Schema(config);

      const id = '1234-5123-2342-1234';
      const item = serializer.serializeItem(s, { id: id });

      item.should.eql({ id: id });
    });

    it('should serialize TimeUUId attribute', () => {
      const config = {
        hashKey: 'timeid',
        schema: {
          timeid: Schema.types.timeUUID(),
        }
      };

      const s = new Schema(config);

      const timeid = '1234-5123-2342-1234';
      const item = serializer.serializeItem(s, { timeid: timeid });

      item.should.eql({ timeid: timeid });
    });

    it('should return null', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          scores: Schema.types.numberSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, null);

      expect(item).to.be.null;
    });

    it('should serialize string attribute for expected', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { name: 'Tim Tester' }, { expected: true });

      item.should.eql({ name: { Value: 'Tim Tester' } });
    });

    it('should serialize string attribute for expected exists false', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { name: { Exists: false } }, { expected: true });

      item.should.eql({ name: { Exists: false } });
    });

    it('should serialize nested attributes', () => {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
          data: {
            first: Joi.string(),
            flag: Joi.boolean(),
            nicks: Schema.types.stringSet(),
          },
        }
      };

      const s = new Schema(config);

      const d = {
        name: 'Foo Bar',
        data: { first: 'Test', flag: true, nicks: ['a', 'b', 'c'] }
      };

      const item = serializer.serializeItem(s, d);

      item.name.should.eql('Foo Bar');
      item.data.first.should.eql('Test');
      item.data.flag.should.eql(true);

      const stringSet = docClient.createSet(['a', 'b', 'c']);

      item.data.nicks.type.should.eql('String');
      item.data.nicks.values.should.eql(stringSet.values);
    });


    it('should return empty when serializing null value', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          names: Schema.types.stringSet(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItem(s, { names: null });

      item.should.eql({});
    });
  });

  describe('#deserializeItem', () => {
    it('should return string value', () => {
      const itemResp = { name: 'Tim Tester' };

      const item = serializer.deserializeItem(itemResp);

      item.name.should.equal('Tim Tester');
    });

    it('should return values in StringSet', () => {
      const itemResp = { names: docClient.createSet(['a', 'b', 'c']) };

      const item = serializer.deserializeItem(itemResp);

      item.names.should.eql(['a', 'b', 'c']);
    });

    it('should return values in NumberSet', () => {
      const itemResp = { scores: docClient.createSet([1, 2, 3]) };

      const item = serializer.deserializeItem(itemResp);

      item.scores.should.eql([1, 2, 3]);
    });

    it('should return null when item is null', () => {
      const item = serializer.deserializeItem(null);

      expect(item).to.be.null;
    });

    it('should return nested values', () => {
      const itemResp = {
        name: 'foo bar',
        scores: docClient.createSet([1, 2, 3]),
        things: [{
          title: 'item 1',
          letters: docClient.createSet(['a', 'b', 'c'])
        }, {
          title: 'item 2',
          letters: docClient.createSet(['x', 'y', 'z'])
        }],
        info: {
          name: 'baz',
          ages: docClient.createSet([20, 21, 22])
        }
      };

      const item = serializer.deserializeItem(itemResp);

      item.should.eql({
        name: 'foo bar',
        scores: [1, 2, 3],
        things: [{
          title: 'item 1',
          letters: ['a', 'b', 'c']
        }, {
          title: 'item 2',
          letters: ['x', 'y', 'z']
        }],
        info: {
          name: 'baz',
          ages: [20, 21, 22]
        }
      });
    });
  });

  describe('#serializeItemForUpdate', () => {
    it('should serialize string attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItemForUpdate(s, 'PUT', { name: 'Tim Tester' });

      item.should.eql({ name: { Action: 'PUT', Value: 'Tim Tester' } });
    });

    it('should serialize number attribute', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItemForUpdate(s, 'PUT', { age: 25 });

      item.should.eql({ age: { Action: 'PUT', Value: 25 } });
    });

    it('should serialize three attributes', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
          scores: Schema.types.numberSet(),
        }
      };

      const s = new Schema(config);

      const attr = { name: 'Tim Test', age: 25, scores: [94, 92, 100] };
      const item = serializer.serializeItemForUpdate(s, 'PUT', attr);

      item.name.should.eql({ Action: 'PUT', Value: 'Tim Test' });
      item.age.should.eql({ Action: 'PUT', Value: 25 });

      const numberSet = docClient.createSet([94, 92, 100]);
      item.scores.Action.should.eql('PUT');
      item.scores.Value.type.should.eql('Number');
      item.scores.Value.values.should.eql(numberSet.values);
    });

    it('should serialize null value to a DELETE action', () => {
      const config = {
        hashKey: 'foo',
        schema: {
          foo: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItemForUpdate(s, 'PUT', { age: null, name: 'Foo Bar' });

      item.should.eql({
        name: { Action: 'PUT', Value: 'Foo Bar' },
        age: { Action: 'DELETE' }
      });
    });

    it('should not serialize hashkey attribute', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItemForUpdate(s, 'PUT', { email: 'test@test.com', name: 'Tim Tester' });

      item.should.eql({ name: { Action: 'PUT', Value: 'Tim Tester' } });
    });

    it('should not serialize hashkey and rangeKey attributes', () => {
      const config = {
        hashKey: 'email',
        rangeKey: 'range',
        schema: {
          email: Joi.string(),
          range: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      const item = serializer.serializeItemForUpdate(s, 'PUT', { email: 'test@test.com', range: 'FOO', name: 'Tim Tester' });

      item.should.eql({ name: { Action: 'PUT', Value: 'Tim Tester' } });
    });

    it('should serialize add operations', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          age: Joi.number(),
          names: Schema.types.stringSet(),
        }
      };

      const s = new Schema(config);

      const update = { email: 'test@test.com', age: { $add: 1 }, names: { $add: ['foo', 'bar'] } };
      const item = serializer.serializeItemForUpdate(s, 'PUT', update);

      item.age.should.eql({ Action: 'ADD', Value: 1 });

      const stringSet = docClient.createSet(['foo', 'bar']);
      item.names.Action.should.eql('ADD');
      item.names.Value.type.should.eql('String');
      item.names.Value.values.should.eql(stringSet.values);
    });

    it('should serialize delete operations', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          names: Schema.types.stringSet(),
          ages: Schema.types.numberSet(),
        }
      };

      const s = new Schema(config);

      const update = { email: 'test@test.com', ages: { $del: [2, 3] }, names: { $del: ['foo', 'bar'] } };
      const item = serializer.serializeItemForUpdate(s, 'PUT', update);

      const stringSet = docClient.createSet(['foo', 'bar']);
      item.names.Action.should.eql('DELETE');
      item.names.Value.type.should.eql('String');
      item.names.Value.values.should.eql(stringSet.values);

      const numberSet = docClient.createSet([2, 3]);
      item.ages.Action.should.eql('DELETE');
      item.ages.Value.type.should.eql('Number');
      item.ages.Value.values.should.eql(numberSet.values);
    });
  });
});
