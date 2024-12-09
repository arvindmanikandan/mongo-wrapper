import events from "events";
import {
  Db,
  MongoClient,
  MongoClientOptions,
  Logger as MongoLogger,
  ObjectId,
  ReadPreference,
  MongoServerSelectionError,
  MongoNetworkError,
  MongoTimeoutError,
} from "mongodb";

export interface Server {
  host: string;
  port: number;
}

export interface AuthConfig {
  username: string;
  password: string;
  authSource?: string;
}

export interface UserConfig {
  db: string;
  auth?: AuthConfig;
  getServers(): Promise<Server[]>;
}

export interface Mongo {
  log(message: string, data?: Record<string, any>): void;
  success(message: string, data?: Record<string, any>): void;
  error(err: Error, data?: Record<string, any>): void;
  connect(): Promise<Mongo>;
  getHealthyHosts(): Server[];
  getClient(): MongoClient;
  reconnecting: Promise<Mongo>;
}

export class MongoConnect implements Mongo {
  name: string;
  emitter: events.EventEmitter;
  mongoClient: MongoClient;
  client: Db;
  userConfig: UserConfig;
  config: MongoClientOptions;
  mode: string;
  reconnecting: Promise<Mongo>;
  private healthyHosts: Server[];

  constructor(
    name: string,
    emitter: events.EventEmitter,
    userConfig: UserConfig,
    mode: string
  ) {
    this.name = name;
    this.emitter = emitter;
    this.userConfig = userConfig;
    this.config = {
      keepAlive: true,
      poolSize: 5,
      connectTimeoutMS: 30000,
      socketTimeoutMS: 30000,
      serverSelectionTimeoutMS: 10000,
      useUnifiedTopology: true,
      connectWithNoPrimary: false,
      readPreference: ReadPreference.SECONDARY_PREFERRED,
    };
    this.config.authSource = (userConfig.auth || {}).authSource;
    this.mode = mode;
  }

  log(message: string, data?: Record<string, any>) {
    this.emitter.emit("log", {
      service: this.name,
      message,
      data,
    });
  }

  success(message: string, data?: Record<string, any>) {
    this.emitter.emit("success", {
      service: this.name,
      message,
      data,
    });
  }

  error(err: Error, data?: Record<string, any>) {
    this.emitter.emit("error", {
      service: this.name,
      data,
      err,
    });
  }

  getHealthyHosts() {
    return this.healthyHosts || [];
  }

  private async getConnectionUrl() {
    let servers = await this.userConfig.getServers();
    const joiner = ["mongodb://"];

    if (this.userConfig.auth) {
      const { username, password } = this.userConfig.auth;
      joiner.push(`${username}:${password}@`);
    }

    // If no active servers, retry with old servers once again
    if (servers.length == 0) {
      servers = this.getHealthyHosts();
    }

    this.healthyHosts = servers;

    joiner.push(
      servers.map((server) => `${server.host}:${server.port}`).join(",")
    );

    return joiner.join("");
  }

  static isValidError(err: Error) {
    return (
      err instanceof MongoServerSelectionError ||
      err instanceof MongoNetworkError ||
      err instanceof MongoTimeoutError
    );
  }

  getClient(): MongoClient {
      return this.mongoClient;
  }

  async connect(): Promise<Mongo> {
    let connected = false;
    // Reconnection handler
    let attempt = 1;
    // Keep reference to old mongoClient, will need to close it later
    const oldMongoClient = this.mongoClient;
    while (!connected && attempt <= 10) {
      try {
        // Returns connection url with only healthy hosts
        const connectionUrl = await this.getConnectionUrl(); // C * 10 => 10C seconds
        const mongoClient = new MongoClient(connectionUrl, { 
          ...this.config,
          appname: this.name,
        }); // 10 * 10 => 100 seconds
        await mongoClient.connect();
        // Update this.mongoClient ONLY after a valid client has been established; else topology closed error will
        // be thrown will is not being monitored/is valid error for reconnection
        this.mongoClient = mongoClient;
        connected = true;
      } catch (err) {
        if (MongoConnect.isValidError(err)) {
          this.error(err);
          // 2 + 4 + 6 + 8 + 10 + 12 ... 20 => 2 * (1 + 2 + 3 + 4 ... 10) => 2 * ((10 * 11) / 2) => 110 seconds
          await new Promise((res) => setTimeout(res, 2 * attempt * 1000));
          attempt++;
        } else {
          throw new Error(err);
        }
      }
    }
    this.client = this.mongoClient.db(this.userConfig.db);
    this.success(`Successfully connected in ${this.mode} mode`);
    MongoLogger.setLevel("info");
    MongoLogger.setCurrentLogger((msg, context) => {
      this.log(msg, context);
    });
    if (oldMongoClient instanceof MongoClient) {
      // Do NOT wait. If you wait, this might block indefinitely due to the older server being out of action.
      oldMongoClient.close();
    }
    return this;
  }
}

export async function handleMongoError(err: Error, mongo: Mongo) {
  if (MongoConnect.isValidError(err)) {
    if (mongo.reconnecting === null) {
      mongo.reconnecting = mongo.connect()
        .then(() => {
          return null;
        });
    }
    await (mongo.reconnecting || Promise.resolve());
    mongo.reconnecting = null;
    return null
  }
  return err;
}

export enum MODES {
  SERVER = "server",
  REPLSET = "replset",
  SHARD = "shard",
}

export interface ServerConfig {
  host: string;
  port: number;
  db: string;
  auth?: AuthConfig;
}

export interface ReplicaConfig {
  db: string;
  replica: {
    name: string;
    servers: Server[];
  };
  auth?: AuthConfig;
}

export interface ShardConfig {
  db: string;
  shard: {
    getServers: () => Promise<Server[]>;
  };
  auth?: AuthConfig;
}

export function MongoFactory(
  mode: MODES,
  name: string,
  emitter: events.EventEmitter,
  config: ServerConfig | ReplicaConfig | ShardConfig,
): Mongo {
  switch (mode) {
    case MODES.SERVER:
      return new ServerMongo(name, emitter, config as ServerConfig);
    case MODES.REPLSET:
      return new ReplSet(name, emitter, config as ReplicaConfig);
    case MODES.SHARD:
      return new ShardMongo(name, emitter, config as ShardConfig);
    default:
      throw new Error("Invalid architecture");
  }
}

class ServerMongo extends MongoConnect {
  constructor(
    name: string,
    emitter: events.EventEmitter,
    config: ServerConfig,
  ) {
    const { db, host, port, auth } = config;
    const userConfig: UserConfig = {
      db,
      getServers: () => Promise.resolve([{ host, port }]),
      auth,
    };
    super(name, emitter, userConfig, MODES.SERVER);
  }
}

class ReplSet extends MongoConnect {
  constructor(
    name: string,
    emitter: events.EventEmitter,
    replicaConfig: ReplicaConfig
  ) {
    const { db, replica, auth } = replicaConfig;
    const config: UserConfig = {
      db: db,
      getServers: () => Promise.resolve(replica.servers),
      auth,
    };
    super(name, emitter, config, MODES.REPLSET);
    this.config.replicaSet = replica.name;
  }
}

class ShardMongo extends MongoConnect {
  constructor(
    name: string,
    emitter: events.EventEmitter,
    shardConfig: ShardConfig
  ) {
    const { db, shard, auth } = shardConfig;
    super(
      name,
      emitter,
      { db, getServers: shard.getServers, auth },
      MODES.SHARD
    );
  }
}


export function MongoFactoryAuto(name: string, emitter: events.EventEmitter, config: MongoConfig) {
  if ((config as ReplicaConfig).replica) {
    return MongoFactory(MODES.REPLSET, name, emitter, config);
  } else if ((config as ShardConfig).shard) {
    return MongoFactory(MODES.SHARD, name, emitter, config);
  } else {
    return MongoFactory(MODES.SERVER, name, emitter, config);
  }
}

export function isValidObjectId(value: string | number | ObjectId) {
  const regex = /[0-9a-f]{24}/;
  const matched = String(value).match(regex);
  if (!matched) {
    return false;
  }

  return ObjectId.isValid(value);
}

export function castToObjectId(value: string) {
  if (isValidObjectId(value) === false) {
    throw new TypeError(`Value passed is not valid objectId, is [ ${value} ]`);
  }
  return ObjectId.createFromHexString(value);
}

export type MongoConfig = ServerConfig | ReplicaConfig | ShardConfig;

export { ObjectId, MongoClient, Db };

