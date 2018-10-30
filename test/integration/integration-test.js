'use strict';

const dynogels = require('../../index');
const chai = require('chai');
const async = require('async');
const _ = require('lodash');
const helper = require('../test-helper');
const uuid = require('uuid');
const Joi = require('joi');

const expect = chai.expect;

chai.should();

let User;
let Tweet;
let Movie;
let DynamicKeyModel;
const internals = {};

internals.userId = n => `userid-${n}`;

internals.loadSeedData = (callback) => {
  callback = callback || _.noop;

  async.parallel([
    (callback) => {
      async.times(15, (n, next) => {
        let roles = ['user'];
        if (n % 3 === 0) {
          roles = ['admin', 'editor'];
        } else if (n % 5 === 0) {
          roles = ['qa', 'dev'];
        }

        User.create({ id: internals.userId(n), email: `test${n}@example.com`, name: `Test ${n % 3}`, age: n + 10, roles: roles }, next);
      }, callback);
    },
    (callback) => {
      async.times(15 * 5, (n, next) => {
        const userId = internals.userId(n % 5);
        const p = { UserId: userId, content: `I love tweeting, in fact Ive tweeted ${n} times`, num: n };
        if (n % 3 === 0) {
          p.tag = '#test';
        }

        return Tweet.create(p, next);
      }, callback);
    },
    (callback) => {
      async.times(10, (n, next) => {
        const director = { firstName: 'Steven', lastName: `Spielberg the ${n}`, titles: ['Producer', 'Writer', 'Director'] };
        const actors = [
          { firstName: 'Tom', lastName: 'Hanks', titles: ['Producer', 'Actor', 'Soundtrack'] }
        ];

        const tags = [`tag ${n}`];

        if (n % 3 === 0) {
          actors.push({ firstName: 'Rex', lastName: 'Ryan', titles: ['Actor', 'Head Coach'] });
          tags.push('Action');
        }

        if (n % 5 === 0) {
          actors.push({ firstName: 'Tom', lastName: 'Coughlin', titles: ['Writer', 'Head Coach'] });
          tags.push('Comedy');
        }

        Movie.create({ title: `Movie ${n}`, releaseYear: 2001 + n, actors: actors, director: director, tags: tags }, next);
      }, callback);
    },
  ], callback);
};

describe('Dynogels Integration Tests', function () {
  this.timeout(0);

  before((done) => {
    dynogels.dynamoDriver(helper.realDynamoDB());

    User = dynogels.define('dynogels-int-test-user', {
      hashKey: 'id',
      schema: {
        id: Joi.string().default(() => uuid.v4(), 'uuid'),
        email: Joi.string().required(),
        name: Joi.string().allow(''),
        age: Joi.number().min(10),
        roles: dynogels.types.stringSet().default(['user']),
        acceptedTerms: Joi.boolean().default(false),
        things: Joi.array(),
        custom: Joi.any().forbidden().error(new Error('Custom field is prohibited')),
        settings: {
          nickname: Joi.string(),
          notify: Joi.boolean().default(true),
          version: Joi.number()
        }

      }
    });

    Tweet = dynogels.define('dynogels-int-test-tweet', {
      hashKey: 'UserId',
      rangeKey: 'TweetID',
      schema: {
        UserId: Joi.string(),
        TweetID: dynogels.types.uuid(),
        content: Joi.string(),
        num: Joi.number(),
        tag: Joi.string(),
        PublishedDateTime: Joi.date().default(Date.now, 'now')
      },
      indexes: [
        { hashKey: 'UserId', rangeKey: 'PublishedDateTime', type: 'local', name: 'PublishedDateTimeIndex' }
      ]
    });

    Movie = dynogels.define('dynogels-int-test-movie', {
      hashKey: 'title',
      timestamps: true,
      schema: {
        title: Joi.string(),
        description: Joi.string(),
        releaseYear: Joi.number(),
        tags: dynogels.types.stringSet(),
        director: Joi.object().keys({
          firstName: Joi.string(),
          lastName: Joi.string(),
          titles: Joi.array()
        }),
        actors: Joi.array().items(Joi.object().keys({
          firstName: Joi.string(),
          lastName: Joi.string(),
          titles: Joi.array()
        }))
      }
    });

    const silentLogger = {
      log: () => {},
      info: () => {},
      warn: () => {},
    };

    DynamicKeyModel = dynogels.define('dynogels-int-test-dyn-key', {
      hashKey: 'id',
      schema: Joi.object().keys({
        id: Joi.string()
      }).unknown(),
      log: silentLogger,
    });

    async.series([
      async.apply(dynogels.createTables.bind(dynogels)),
      (callback) => {
        const items = [{ fiz: 3, buz: 5, fizbuz: 35 }];
        User.create({ id: '123456789', email: 'some@user.com', age: 30, settings: { nickname: 'thedude' }, things: items }, callback);
      },
      (callback) => {
        User.create({ id: '9999', email: '9999@test.com', age: 99, name: 'Nancy Nine' }, callback);
      },
      internals.loadSeedData
    ], done);
  });

  describe('#create', () => {
    it('should create item with hash key', (done) => {
      User.create({
        email: 'foo@bar.com',
        age: 18,
        roles: ['user', 'admin'],
        acceptedTerms: true,
        settings: {
          nickname: 'fooos',
          version: 2
        }
      }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings']);
        return done();
      });
    });

    it('should create item with empty string', (done) => {
      User.create({
        email: 'foo2@bar.com',
        name: '',
        age: 22,
        roles: ['user'],
        settings: {
          nickname: 'foo2',
          version: 2
        }
      }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings']);
        return done();
      });
    });

    it('should return condition exception when using ConditionExpression', (done) => {
      const item = { email: 'test123@test.com', age: 33, roles: ['user'] };

      User.create(item, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('test123@test.com');

        const params = {};
        params.ConditionExpression = '#i <> :x';
        params.ExpressionAttributeNames = { '#i': 'id' };
        params.ExpressionAttributeValues = { ':x': acc.get('id') };

        const item2 = _.merge(item, { id: acc.get('id') });
        User.create(item2, params, (error, acc) => {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should return condition exception when using expected shorthand', (done) => {
      const item = { email: 'test444@test.com', age: 33, roles: ['user'] };

      User.create(item, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('test444@test.com');

        const opts = { expected: { email: 'foo@bar.com' } };

        const item2 = _.merge(item, { id: acc.get('id') });
        User.create(item2, opts, (error, acc) => {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should return condition exception when using overwrite shorthand', (done) => {
      const item = { email: 'testOverwrite@test.com', age: 20 };

      User.create(item, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        const item2 = _.merge(item, { id: acc.get('id') });
        User.create(item2, { overwrite: false }, (error, acc) => {
          expect(error).to.exist;
          expect(error.code).to.eql('ConditionalCheckFailedException');
          expect(acc).to.not.exist;

          return done();
        });
      });
    });

    it('should return custom errors specified in the schema', (done) => {
      const item = { id: '123456789', email: 'newemail', custom: 'forbidden' };
      User.create(item, (err, acc) => {
        expect(err).to.exist;
        expect(acc).to.not.exist;
        expect(err).to.match(/Custom field is prohibited/);
        done();
      });
    });

    it('should create item with dynamic keys', (done) => {
      DynamicKeyModel.create({
        id: 'rand-1',
        name: 'Foo Bar',
        children: ['sam', 'steve', 'sarah', 'sally'],
        settings: { nickname: 'Tester', info: { color: 'green', age: 19 } }
      }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'name', 'children', 'settings']);
        return done();
      });
    });

    it('should create multiple items at once', (done) => {
      const item = { email: 'testMulti1@test.com', age: 10 };
      const item2 = { email: 'testMulti2@test.com', age: 20 };
      const item3 = { email: 'testMulti3@test.com', age: 30 };

      User.create([item, item2, item3], (err, accounts) => {
        expect(err).to.not.exist;
        expect(accounts).to.exist;
        expect(accounts).to.have.length(3);

        return done();
      });
    });
  });

  describe('#get', () => {
    it('should get item by hash key', (done) => {
      User.get({ id: '123456789' }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings', 'things']);
        return done();
      });
    });

    it('should get return selected attributes AttributesToGet param', (done) => {
      User.get({ id: '123456789' }, { AttributesToGet: ['email', 'age'] }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['email', 'age']);
        return done();
      });
    });

    it('should get return selected attributes using ProjectionExpression param', (done) => {
      User.get({ id: '123456789' }, { ProjectionExpression: 'email, age, settings.nickname' }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['email', 'age', 'settings']);
        expect(acc.get('settings').nickname).to.exist;
        return done();
      });
    });
  });

  describe('#update', () => {
    it('should update item appended role', (done) => {
      User.update({
        id: '123456789',
        roles: { $add: 'tester' }
      }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'settings', 'things']);
        expect(acc.get('roles').sort()).to.eql(['tester', 'user']);

        return done();
      });
    });

    it('should use expected to check that the item exists', (done) => {
      User.update(
        {
          id: '123456789',
          email: 'updated_already@exists.com'
        },
        {
          expected: { id: { Exists: true } }
        },
        (err, acc) => {
          expect(err).to.not.exist;
          expect(acc).to.exist;
          expect(acc.attrs.email).to.eql('updated_already@exists.com');
          done();
        }
      );
    });

    it('should fail when expected exists check fails', (done) => {
      User.update(
        {
          id: 'does not exist'
        },
        { expected: { id: { Exists: true } } },
        (err, acc) => {
          expect(err).to.exist;
          expect(acc).to.not.exist;
          done();
        }
      );
    });

    it('should remove name attribute from user record when set to empty string', (done) => {
      User.update({ id: '9999', name: '' }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms']);
        return done();
      });
    });

    it('should update age using expected value', (done) => {
      User.update({ id: '9999', age: 100 }, { expected: { age: 99 } }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get('age')).to.eql(100);
        return done();
      });
    });

    it('should update email using expected that an email already exists', (done) => {
      User.update({ id: '9999', email: 'new9999@test.com' }, { expected: { email: { Exists: true } } }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get('email')).to.eql('new9999@test.com');
        return done();
      });
    });

    it('should remove settings attribute from user record', (done) => {
      User.update({ id: '123456789', settings: null }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;

        expect(acc.get()).to.have.keys(['id', 'email', 'age', 'roles', 'acceptedTerms', 'things']);
        return done();
      });
    });

    it('should update User using updateExpression', (done) => {
      const params = {};
      params.UpdateExpression = 'ADD #a :x SET things[0].buz = :y';
      params.ConditionExpression = '#a = :current';
      params.ExpressionAttributeNames = { '#a': 'age' };
      params.ExpressionAttributeValues = { ':x': 1, ':y': 22, ':current': 30 };

      User.update({ id: '123456789' }, params, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('age')).to.equal(31);
        expect(acc.get('things')).to.eql([{ fiz: 3, buz: 22, fizbuz: 35 }]);
        return done();
      });
    });

    it('should update Movie using updateExpressions', (done) => {
      const params = {};
      params.UpdateExpression = 'SET #year = #year + :inc, #dir.titles = list_append(#dir.titles, :title), #act[0].firstName = :firstName ADD tags :tag';
      params.ConditionExpression = '#year = :current';
      params.ExpressionAttributeNames = {
        '#year': 'releaseYear',
        '#dir': 'director',
        '#act': 'actors'
      };

      params.ExpressionAttributeValues = {
        ':inc': 1,
        ':current': 2001,
        ':title': ['The Man'],
        ':firstName': 'Rob',
        ':tag': dynogels.Set(['Sports', 'Horror'], 'S')
      };

      Movie.update({ title: 'Movie 0', description: 'This is a description' }, params, (err, mov) => {
        expect(err).to.not.exist;

        expect(mov.get('description')).to.eql('This is a description');
        expect(mov.get('releaseYear')).to.eql(2002);
        expect(mov.get('updatedAt')).to.exist;
        return done();
      });
    });

    it('should update item with dynamic keys', (done) => {
      DynamicKeyModel.update({
        id: 'rand-5',
        color: 'green',
        settings: { email: 'dynupdate@test.com' }
      }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get()).to.have.keys(['id', 'settings', 'color']);

        expect(acc.get()).to.eql({
          id: 'rand-5',
          color: 'green',
          settings: { email: 'dynupdate@test.com' }
        });

        return done();
      });
    });

    it('should fail to update an attribute not in the schema', (done) => {
      User.update({
        id: '123456789',
        invalidAttribute: 'Invalid Value'
      }, (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        return done();
      });
    });

    it('should fail to remove a required attribute', (done) => {
      User.update({
        id: '123456789',
        email: null
      }, (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

    it('should successfully remove an optional attribute', (done) => {
      User.update({
        id: '123456789',
        name: null,
      }, (err, acc) => {
        expect(err).to.be.null;
        expect(acc).to.exist;
        done();
      });
    });

    it('should fail for attribute mismatch to schema type', (done) => {
      User.update({
        id: '123456789',
        name: 1
      }, (err, account) => {
        expect(err).to.exist;
        expect(account).to.not.exist;
        done();
      });
    });

    it('should fail to use $add for an invalid attribute', (done) => {
      User.update({
        id: '123456789',
        name: { $add: '$addname' }
      }, (err, acc) => {
        expect(err).to.exist;
        expect(acc).to.not.exist;
        done();
      });
    });
  });

  describe('#getItems', () => {
    it('should return 3 items', (done) => {
      User.getItems(['userid-1', 'userid-2', 'userid-3'], (err, accounts) => {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(3);
        return done();
      });
    });

    it('should return 2 items with only selected attributes', (done) => {
      const opts = { AttributesToGet: ['email', 'age'] };

      User.getItems(['userid-1', 'userid-2'], opts, (err, accounts) => {
        expect(err).to.not.exist;
        expect(accounts).to.have.length(2);
        _.each(accounts, (acc) => {
          expect(acc.get()).to.have.keys(['email', 'age']);
        });

        return done();
      });
    });
  });

  describe('#query', () => {
    it('should return users tweets', (done) => {
      Tweet.query('userid-1').exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('UserId')).to.eql('userid-1');
        });

        return done();
      });
    });

    it('should return users tweets with specific attributes', (done) => {
      Tweet.query('userid-1').attributes(['num', 'content']).exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('UserId')).to.not.exist;
          expect(t.get()).to.include.keys('num', 'content');
        });

        return done();
      });
    });

    it('should return tweets using secondaryIndex', (done) => {
      Tweet.query('userid-1')
        .usingIndex('PublishedDateTimeIndex')
        .consistentRead(true)
        .descending()
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          let prev;
          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');

            const published = t.get('PublishedDateTime');

            if (prev) {
              expect(published < prev).to.be.true;
            }

            prev = published;
          });

          return done();
        });
    });

    it('should return tweets using secondaryIndex and date object', (done) => {
      const oneMinAgo = new Date(new Date().getTime() - (60 * 1000));

      Tweet.query('userid-1')
        .usingIndex('PublishedDateTimeIndex')
        .where('PublishedDateTime').gt(oneMinAgo)
        .descending()
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          let prev;
          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');

            const published = t.get('PublishedDateTime');

            if (prev) {
              expect(published < prev).to.be.true;
            }

            prev = published;
          });

          return done();
        });
    });

    it('should return tweets that match filters', (done) => {
      Tweet.query('userid-1')
        .filter('num').between(4, 8)
        .filter('tag').exists()
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');
            expect(t.get('num')).to.be.above(3);
            expect(t.get('num')).to.be.below(9);
            expect(t.get('tag')).to.exist;
          });

          return done();
        });
    });

    it('should return tweets that match exists filter', (done) => {
      Tweet.query('userid-1')
        .filter('tag').exists()
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');
            expect(t.get('tag')).to.exist;
          });

          return done();
        });
    });

    it('should return tweets that match IN filter', (done) => {
      Tweet.query('userid-1')
        .filter('num').in([4, 6, 8])
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');
            const c = _.includes([4, 6, 8], t.get('num'));
            expect(c).to.be.true;
          });

          return done();
        });
    });

    it('should return tweets that match expression filters', (done) => {
      Tweet.query('userid-1')
        .filterExpression('#num BETWEEN :low AND :high AND attribute_exists(#tag)')
        .expressionAttributeValues({ ':low': 4, ':high': 8 })
        .expressionAttributeNames({ '#num': 'num', '#tag': 'tag' })
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (t) => {
            expect(t.get('UserId')).to.eql('userid-1');
            expect(t.get('num')).to.be.above(3);
            expect(t.get('num')).to.be.below(9);
            expect(t.get('tag')).to.exist;
          });

          return done();
        });
    });

    it('should return tweets with projection expression', (done) => {
      Tweet.query('userid-1')
        .projectionExpression('#con, UserId')
        .expressionAttributeNames({ '#con': 'content' })
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (t) => {
            expect(t.get()).to.have.keys(['content', 'UserId']);
          });

          return done();
        });
    });

    it('should return all tweets from user', (done) => {
      Tweet.query('userid-1').limit(2).loadAll().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('UserId')).to.eql('userid-1');
        });

        return done();
      });
    });

    it('should return movie if directed by Steven Spielberg the 4', (done) => {
      Movie.query('Movie 4').filter('director.firstName').equals('Steven').filter('director.lastName').equals('Spielberg the 4').limit(2).loadAll().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('title')).to.eql('Movie 4');

          expect(t.get('director').firstName).to.eql('Steven');
          expect(t.get('director').lastName).to.eql('Spielberg the 4');
        });

        return done();
      });
    });
  });


  describe('#scan', () => {
    it('should return all users', (done) => {
      User.scan().loadAll().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return all users with limit', (done) => {
      User.scan().limit(2).loadAll().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return users with specific attributes', (done) => {
      User.scan()
        .where('age').gt(18)
        .attributes(['email', 'roles', 'age']).exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);
          _.each(data.Items, (u) => {
            expect(u.get()).to.include.keys('email', 'roles', 'age');
          });

          return done();
        });
    });

    it('should return 10 users', (done) => {
      User.scan().limit(10).exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length(10);

        return done();
      });
    });

    it('should return users older than 18', (done) => {
      User.scan()
        .where('age').gt(18)
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('age')).to.be.above(18);
          });

          return done();
        });
    });

    it('should return users matching multiple filters', (done) => {
      User.scan()
        .where('age').between(18, 22)
        .where('email').beginsWith('test1')
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('age')).to.be.within(18, 22);
            expect(u.get('email')).to.match(/^test1.*/);
          });

          return done();
        });
    });

    it('should return users contains admin role', (done) => {
      User.scan()
        .where('roles').contains('admin')
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('roles')).to.include('admin');
          });

          return done();
        });
    });

    it('should return users using stream interface', (done) => {
      const stream = User.scan().exec();

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

    it('should return users that match expression filters', (done) => {
      User.scan()
        .filterExpression('#age BETWEEN :low AND :high AND begins_with(#email, :e)')
        .expressionAttributeValues({ ':low': 18, ':high': 22, ':e': 'test1' })
        .expressionAttributeNames({ '#age': 'age', '#email': 'email' })
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('age')).to.be.within(18, 22);
            expect(u.get('email')).to.match(/^test1.*/);
          });

          return done();
        });
    });

    it('should return users between ages', (done) => {
      User.scan()
        .where('age').between(18, 22)
        .where('email').beginsWith('test1')
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('age')).to.be.within(18, 22);
            expect(u.get('email')).to.match(/^test1.*/);
          });

          return done();
        });
    });

    it('should return users matching IN filter', (done) => {
      User.scan()
        .where('age').in([2, 9, 20])
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            const c = _.includes([2, 9, 20], u.get('age'));
            expect(c).to.be.true;
          });

          return done();
        });
    });

    it('should return users with projection expression', (done) => {
      User.scan()
        .projectionExpression('age, email, #roles')
        .expressionAttributeNames({ '#roles': 'roles' })
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get()).to.have.keys(['age', 'email', 'roles']);
          });

          return done();
        });
    });

    it('should load all users with limit', (done) => {
      User.scan().loadAll().limit(2).exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return users using stream interface and limit', (done) => {
      const stream = User.scan().loadAll().limit(2).exec();

      let called = false;
      stream.on('readable', () => {
        called = true;
        const data = stream.read();

        if (data) {
          expect(data.Items).to.have.length.within(0, 2);
        }
      });

      stream.on('end', () => {
        expect(called).to.be.true;
        return done();
      });
    });

    it('should load tweets using not null tag clause', (done) => {
      Tweet.scan().where('tag').notNull().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('tag')).to.exist;
        });

        return done();
      });
    });

    it('should return all movies directed by Steven Spielberg the 4', (done) => {
      Movie.scan().where('director.firstName').equals('Steven').where('director.lastName').equals('Spielberg the 4').limit(2).loadAll().exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        _.each(data.Items, (t) => {
          expect(t.get('director').firstName).to.eql('Steven');
          expect(t.get('director').lastName).to.eql('Spielberg the 4');
        });

        return done();
      });
    });
  });

  describe('#parallelScan', () => {
    it('should return all users', (done) => {
      User.parallelScan(4).exec((err, data) => {
        expect(err).to.not.exist;
        expect(data.Items).to.have.length.above(0);

        return done();
      });
    });

    it('should return users older than 18', (done) => {
      User.parallelScan(4)
        .where('age').gt(18)
        .exec((err, data) => {
          expect(err).to.not.exist;
          expect(data.Items).to.have.length.above(0);

          _.each(data.Items, (u) => {
            expect(u.get('age')).to.be.above(18);
          });

          return done();
        });
    });

    it('should return users using stream interface', (done) => {
      const stream = User.parallelScan(4).exec();

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


  describe('timestamps', () => {
    let Model;
    let ModelCustomTimestamps;

    before((done) => {
      Model = dynogels.define('dynogels-int-test-timestamp', {
        hashKey: 'id',
        timestamps: true,
        schema: {
          id: Joi.string()
        }
      });

      ModelCustomTimestamps = dynogels.define('dynogels-int-test-timestamp-custom', {
        hashKey: 'id',
        timestamps: true,
        createdAt: 'created',
        updatedAt: 'updated',
        schema: {
          id: Joi.string()
        }
      });


      return dynogels.createTables(done);
    });

    it('should add createdAt param', (done) => {
      Model.create({ id: 'test-1' }, (err) => {
        expect(err).to.not.exist;

        Model.get('test-1', (err2, data) => {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-1');
          expect(data.get('createdAt')).to.exist;

          return done();
        });
      });
    });

    it('should add updatedAt param', (done) => {
      Model.update({ id: 'test-2' }, (err) => {
        expect(err).to.not.exist;

        Model.get('test-2', (err2, data) => {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-2');
          expect(data.get('updatedAt')).to.exist;

          return done();
        });
      });
    });

    it('should add custom createdAt param', (done) => {
      ModelCustomTimestamps.create({ id: 'test-1' }, (err) => {
        expect(err).to.not.exist;

        ModelCustomTimestamps.get('test-1', (err2, data) => {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-1');
          expect(data.get('created')).to.exist;

          return done();
        });
      });
    });

    it('should add custom updatedAt param', (done) => {
      ModelCustomTimestamps.update({ id: 'test-2' }, (err) => {
        expect(err).to.not.exist;

        ModelCustomTimestamps.get('test-2', (err2, data) => {
          expect(err2).to.not.exist;

          expect(data.get('id')).to.eql('test-2');
          expect(data.get('updated')).to.exist;

          return done();
        });
      });
    });
  });

  describe('#destroy', () => {
    let userId;
    beforeEach((done) => {
      User.create({ email: 'destroy@test.com', age: 20, roles: ['tester'] }, (err, acc) => {
        expect(err).to.not.exist;
        userId = acc.get('id');

        return done();
      });
    });

    it('should destroy item with hash key', (done) => {
      User.destroy({ id: userId }, (err) => {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('should destroy item and return old values', (done) => {
      User.destroy({ id: userId }, { ReturnValues: 'ALL_OLD' }, (err, acc) => {
        expect(err).to.not.exist;
        expect(acc).to.exist;
        expect(acc.get('email')).to.eql('destroy@test.com');
        return done();
      });
    });

    it('should return condition exception when using ConditionExpression', (done) => {
      const params = {};
      params.ConditionExpression = '#i = :x';
      params.ExpressionAttributeNames = { '#i': 'id' };
      params.ExpressionAttributeValues = { ':x': 'dontexist' };

      User.destroy({ id: 'dontexist' }, params, (err, acc) => {
        expect(err).to.exist;
        expect(err.code).to.eql('ConditionalCheckFailedException');
        expect(acc).to.not.exist;

        return done();
      });
    });

    it('should return condition exception when using Expected shorthand', (done) => {
      const opts = { expected: { id: 'dontexist' } };

      User.destroy({ id: 'dontexist' }, opts, (err, acc) => {
        expect(err).to.exist;
        expect(err.code).to.eql('ConditionalCheckFailedException');
        expect(acc).to.not.exist;

        return done();
      });
    });
  });


  describe('model methods', () => {
    it('#save with passed in attributes', (done) => {
      const t = new Tweet({
        UserId: 'tester-1',
        content: 'save test tweet',
        tag: 'test'
      });

      t.save((err) => {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('#save without passed in attributes', (done) => {
      const t = new Tweet();

      const attrs = { UserId: 'tester-1', content: 'save test tweet', tag: 'test' };
      t.set(attrs);

      t.save((err) => {
        expect(err).to.not.exist;
        return done();
      });
    });

    it('#save without callback', (done) => {
      const t = new Tweet({
        UserId: 'tester-1',
        content: 'save test tweet',
        tag: 'test'
      });

      t.save();

      return done();
    });

    it('#update with callback', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        tweet.set({ tag: 'update' });

        tweet.update((err) => {
          expect(err).to.not.exist;
          expect(tweet.get('tag')).to.eql('update');
          return done();
        });
      });
    });

    it('#update without callback', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        tweet.set({ tag: 'update' });

        tweet.update();

        return done();
      });
    });


    it('#destroy with callback', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        tweet.destroy((err) => {
          expect(err).to.not.exist;
          return done();
        });
      });
    });

    it('#destroy without callback', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        tweet.destroy();

        return done();
      });
    });

    it('#toJSON', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        expect(tweet.toJSON()).to.have.keys(['UserId', 'content', 'TweetID', 'PublishedDateTime']);
        return done();
      });
    });

    it('#toPlainObject', (done) => {
      Tweet.create({ UserId: 'tester-2', content: 'update test tweet' }, (err, tweet) => {
        expect(err).to.not.exist;

        expect(tweet.toPlainObject()).to.have.keys(['UserId', 'content', 'TweetID', 'PublishedDateTime']);
        return done();
      });
    });
  });
});
