

const { MongoClient, Logger: mLogger } = require('mongodb');
const Service = require('@akshendra/service');
const misc = require('@akshendra/misc');
const { validate, joi } = require('@akshendra/validator');

const { is } = misc;

/**
 * @class Mongo
 */
class Mongo extends Service {
  /**
   * @param {string} name - unique name to this service
   * @param {EventEmitter} emitter
   * @param {Object} config - configuration object of service
   */
  constructor(name, emitter, config) {
    super(name, emitter);

    this.client = null;
    this.config = validate(config, joi.object().keys({
      host: joi.string().default('127.0.0.1'),
      port: joi.number().integer().min(0).default(27017),
      db: joi.string().default('test'),
      replica: joi.object().keys({
        use: joi.bool().default(true),
        servers: joi.array().items(joi.object().keys({
          host: joi.string().default('127.0.0.1'),
          port: joi.number().integer().min(0).default(27017),
        })),
        name: joi.string().default('test'),
      }).default({
        use: false,
      }),
      auth: joi.object().keys({
        use: joi.bool().default(true),
        username: joi.string().default('admin'),
        password: joi.string().default('pass'),
        authSource: joi.string().default('admin'),
      }).default({
        use: false,
      }),
      options: joi.object().keys({
        keepAlive: joi.number().integer().default(1000),
        autoReconnect: joi.bool().default(true),
        poolSize: joi.number().integer().default(5),
        connectTimeoutMS: joi.number().integer().default(30000),
        socketTimeoutMS: joi.number().integer().default(30000),
        connectWithNoPrimary: joi.number().integer().default(false),
        readPreference: joi.string().valid([
          'primary',
          'primaryPreferred',
          'secondary',
          'secondaryPreferred',
          'nearest',
        ]).default('primary'),
      }).default({
        keepAlive: 1000,
        autoReconnect: true,
        poolSize: 5,
        connectTimeoutMS: 30000,
        socketTimeoutMS: 30000,
        connectWithNoPrimary: false,
        readPreference: 'secondaryPreferred',
      }),
    }));
  }


  /**
   * Connect to server
   */
  init() {
    const { config } = this;
    const { auth, options, replica } = config;

    if (is.not.null(this.client)) {
      return Promise.resolve(this);
    }

    const infoObj = {};

    let url = 'mongodb://';
    if (auth.use === true) {
      Object.assign(infoObj, {
        authentication: 'TRUE',
      });
      url += `${auth.username}:${auth.password}@`;
      Object.assign(options, {
        authSource: auth.authSource,
      });
    } else {
      Object.assign(infoObj, {
        authentication: 'FALSE',
      });
    }
    if (replica.use === true) {
      Object.assign(infoObj, {
        mode: 'REPLICAS',
        servers: replica.servers,
      });
      url += replica.servers.map(s => `${s.host}:${s.port}`).join(',');
      Object.assign(options, {
        replicaSet: replica.name,
      });
    } else {
      Object.assign(infoObj, {
        mode: 'SINGLE',
        host: config.host,
        port: config.port,
      });
      url += `${config.host}:${config.port}`;
    }
    Object.assign(infoObj, {
      db: config.db,
      options,
    });

    url += `/?appname=${this.name}`;

    this.log.info('Connecting to url', url);
    this.emitInfo('connecting', `Connecting in ${infoObj.mode} mode`, infoObj);

    return MongoClient.connect(url, options).then(client => {
      this.client = client.db(config.db);
      this.connected = true;
      const message = 'Successfully connected';
      this.log.info(message);
      this.emitSuccess(`Successfully connected in ${infoObj.mode} mode`);
      mLogger.setLevel('info');
      mLogger.setCurrentLogger((msg, context) => {
        this.emitInfo('event', msg, context);
      });
      return this;
    });
  }

  collection(name) {
    return this.client.collection(name);
  }

  mapIds(ids) { // eslint-disable-line
    return ids.map(misc.castToObjectId.bind(misc));
  }

  idQuery(id) { // eslint-disable-line
    return {
      _id: misc.castToObjectId(id),
    };
  }

  inQuery(ids) {
    return {
      _id: {
        $in: this.mapIds(ids),
      },
    };
  }

  checkItem(model, item, id, throws) { // eslint-disable-line
    if (throws === true && (is.not.existy(item) || is.empty(item))) {
      throw ReferenceError(`${model} not find ${id},`);
    }
    return item;
  }

  getOneById(col, id) {
    return this.collection(col).findOne(this.idQuery(id));
  }

  getOne(col, query) {
    return this.collection(col).findOne(query);
  }

  getManyByIds(col, ids) {
    return this.collection(col).find(this.inQuery(ids)).toArray();
  }

  getMany(col, query) {
    return this.collection(col).find(query).toArray();
  }

  insertOne(col, doc) {
    return this.collection(col).insert(doc);
  }

  upsertOne(col, query, doc) {
    return this.collection(col).updateOne(query, {
      $set: doc,
    }, { upsert: true });
  }

  // insert an array of docs
  insertMany(col, docs) {
    return this.collection(col).insertMany(docs);
  }

  updateOrUpsertMany(col, docs) {
    const bulk = this.collection(col).initializeUnorderedBulkOp();
    docs.forEach((doc) => {
      bulk.find({ _id: doc._id }).upsert().updateOne({ $set: doc });
    });
    if (docs.length > 0) {
      return bulk.execute()
        .catch((ex) => {
          if (ex.message.indexOf('E11000 duplicate key error collection') !== -1) {
            return this.updateOrUpsertMany(col, docs);
          }
          throw ex;
        });
    }
    const error = new Error('Noting to insert');
    return Promise.reject(error);
  }

  doesExist(col, id) {
    const query = {
      _id: misc.castToObjectId(id),
    };
    return this.collection(col).findOne(query)
      .then((doc) => {
        if (is.not.existy(doc) || is.empty(doc)) {
          return false;
        }
        return true;
      });
  }

  setOfExistence(col, ids) {
    return this.getManyByIds(col, ids)
      .then((docs) => {
        const set = new Set();
        docs.forEach((doc) => {
          set.add(String(doc._id));
        });
        return set;
      });
  }

  cursor(col) {
    return this.collection(col).find({});
  }
}

module.exports = Mongo;
