'use strict';

const dynogels = require('../index');
const util = require('util');
const _ = require('lodash');
const async = require('async');
const Joi = require('joi');

const AWS = dynogels.AWS;
AWS.config.loadFromPath(`${process.env.HOME}/.ec2/credentials.json`);

const Movie = dynogels.define('example-nested-attribute', {
  hashKey: 'title',
  timestamps: true,
  schema: {
    title: Joi.string(),
    releaseYear: Joi.number(),
    tags: dynogels.types.stringSet(),
    director: Joi.object().keys({
      firstName: Joi.string(),
      lastName: Joi.string(),
      titles: Joi.array()
    }),
    actors: Joi.array().includes(Joi.object().keys({
      firstName: Joi.string(),
      lastName: Joi.string(),
      titles: Joi.array()
    }))
  }
});

const printResults = (err, data) => {
  console.log('----------------------------------------------------------------------');
  if (err) {
    console.log('Error - ', err);
  } else {
    console.log('Movie - ', util.inspect(data.get(), { depth: null }));
  }
  console.log('----------------------------------------------------------------------');
};

const loadSeedData = (callback) => {
  callback = callback || _.noop;

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
};

const runExample = () => {
  Movie.create({
    title: 'Star Wars: Episode IV - A New Hope',
    releaseYear: 1977,
    director: {
      firstName: 'George', lastName: 'Lucas', titles: ['Director']
    },
    actors: [
      { firstName: 'Mark', lastName: 'Hamill', titles: ['Actor'] },
      { firstName: 'Harrison', lastName: 'Ford', titles: ['Actor', 'Producer'] },
      { firstName: 'Carrie', lastName: 'Fisher', titles: ['Actress', 'Writer'] },
    ],
    tags: ['Action', 'Adventure']
  }, printResults);

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

  Movie.update({ title: 'Movie 0' }, params, printResults);
};

async.series([
  async.apply(dynogels.createTables.bind(dynogels)),
  loadSeedData
], (err) => {
  if (err) {
    console.log('error', err);
    process.exit(1);
  }

  runExample();
});
