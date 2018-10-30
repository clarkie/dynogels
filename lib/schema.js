'use strict';

const Joi = require('joi');
const nodeUUID = require('uuid');
const _ = require('lodash');

const internals = {};

internals.secondaryIndexSchema = Joi.object().keys({
  hashKey: Joi.string().when('type', { is: 'local', then: Joi.ref('$hashKey'), otherwise: Joi.required() }),
  rangeKey: Joi.string().when('type', { is: 'local', then: Joi.required(), otherwise: Joi.optional() }),
  type: Joi.string().valid('local', 'global').required(),
  name: Joi.string().required(),
  projection: Joi.object(),
  readCapacity: Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise: Joi.forbidden() }),
  writeCapacity: Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise: Joi.forbidden() })
});

internals.configSchema = Joi.object().keys({
  hashKey: Joi.string().required(),
  rangeKey: Joi.string(),
  tableName: Joi.alternatives().try(Joi.string(), Joi.func()),
  indexes: Joi.array().items(internals.secondaryIndexSchema),
  schema: Joi.object(),
  timestamps: Joi.boolean().default(false),
  createdAt: Joi.alternatives().try(Joi.string(), Joi.boolean()),
  updatedAt: Joi.alternatives().try(Joi.string(), Joi.boolean()),
  log: Joi.object({
    info: Joi.func(),
    warn: Joi.func(),
  }).optional().unknown(),

  // based on Joi internals
  validation: {
    abortEarly: Joi.boolean(),
    convert: Joi.boolean(),
    allowUnknown: Joi.boolean(),
    skipFunctions: Joi.boolean(),
    stripUnknown: Joi.boolean(),
    language: Joi.object(),
    presence: Joi.string().allow('optional', 'required', 'forbidden', 'ignore'),
    strip: Joi.boolean(),
    noDefaults: Joi.boolean()
  }
}).required();

internals.wireType = (key) => {
  switch (key) {
    case 'string':
      return 'S';
    case 'date':
      return 'DATE';
    case 'number':
      return 'N';
    case 'boolean':
      return 'BOOL';
    case 'binary':
      return 'B';
    case 'array':
      return 'L';
    default:
      return null;
  }
};

internals.findDynamoTypeMetadata = (data) => {
  const meta = _.find(data.meta, data => _.isString(data.dynamoType));

  if (meta) {
    return meta.dynamoType;
  } else {
    return internals.wireType(data.type);
  }
};

internals.parseDynamoTypes = (data) => {
  if (_.isPlainObject(data) && data.type === 'object' && _.isPlainObject(data.children)) {
    return internals.parseDynamoTypes(data.children);
  }

  const mapped = _.reduce(data, (result, val, key) => {
    if (val.type === 'object' && _.isPlainObject(val.children)) {
      result[key] = internals.parseDynamoTypes(val.children);
    } else {
      result[key] = internals.findDynamoTypeMetadata(val);
    }

    return result;
  }, {});

  return mapped;
};

const Schema = module.exports = function (config) {
  this.secondaryIndexes = {};
  this.globalIndexes = {};
  this.validationOptions = config.validation;

  const context = { hashKey: config.hashKey };

  const self = this;
  Joi.validate(config, internals.configSchema, { context: context }, (err, data) => {
    if (err) {
      const msg = 'Invalid table schema, check your config ';
      throw new Error(msg + err.annotate());
    }

    self.hashKey = data.hashKey;
    self.rangeKey = data.rangeKey;
    self.tableName = data.tableName;
    self.timestamps = data.timestamps;
    self.createdAt = data.createdAt;
    self.updatedAt = data.updatedAt;

    if (data.indexes) {
      self.globalIndexes = _.chain(data.indexes).filter({ type: 'global' }).keyBy('name').value();
      self.secondaryIndexes = _.chain(data.indexes).filter({ type: 'local' }).keyBy('name').value();
    }

    if (data.schema) {
      self._modelSchema = _.isPlainObject(data.schema) ? Joi.object().keys(data.schema) : data.schema;
    } else {
      self._modelSchema = Joi.object();
    }

    if (self.timestamps) {
      const valids = {};
      let createdAtParamName = 'createdAt';
      let updatedAtParamName = 'updatedAt';

      if (self.createdAt) {
        if (_.isString(self.createdAt)) {
          createdAtParamName = self.createdAt;
        }
      }

      if (self.updatedAt) {
        if (_.isString(self.updatedAt)) {
          updatedAtParamName = self.updatedAt;
        }
      }

      if (self.createdAt !== false) {
        valids[createdAtParamName] = Joi.date();
      }

      if (self.updatedAt !== false) {
        valids[updatedAtParamName] = Joi.date();
      }

      const extended = self._modelSchema.keys(valids);

      self._modelSchema = extended;
    }

    self._modelDatatypes = internals.parseDynamoTypes(self._modelSchema.describe());
  });
};

Schema.types = {};

Schema.types.stringSet = () => {
  const set = Joi.array().items(Joi.string()).meta({ dynamoType: 'SS' });

  return set;
};

Schema.types.numberSet = () => {
  const set = Joi.array().items(Joi.number()).meta({ dynamoType: 'NS' });
  return set;
};

Schema.types.binarySet = () => {
  const set = Joi.array().items(Joi.binary(), Joi.string()).meta({ dynamoType: 'BS' });
  return set;
};

Schema.types.uuid = () => Joi.string().guid().default(() => nodeUUID.v4(), 'uuid v4');

Schema.types.timeUUID = () => Joi.string().guid().default(() => nodeUUID.v1(), 'uuid v1');

Schema.prototype.validate = function (params, options) {
  options = options || {};
  if (this.validationOptions) {
    _.extend(options, this.validationOptions);
  }

  return Joi.validate(params, this._modelSchema, options);
};

internals.invokeDefaultFunctions = data => _.mapValues(data, (val) => {
  if (_.isPlainObject(val)) {
    return internals.invokeDefaultFunctions(val);
  } else {
    return val;
  }
});

Schema.prototype.applyDefaults = function (data) {
  const result = this.validate(data, { abortEarly: false });

  return internals.invokeDefaultFunctions(result.value);
};
