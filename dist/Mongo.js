"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Db = exports.MongoClient = exports.ObjectId = exports.MODES = exports.MongoConnect = void 0;
exports.handleMongoError = handleMongoError;
exports.MongoFactory = MongoFactory;
exports.MongoFactoryAuto = MongoFactoryAuto;
exports.isValidObjectId = isValidObjectId;
exports.castToObjectId = castToObjectId;
const mongodb_1 = require("mongodb");
Object.defineProperty(exports, "Db", { enumerable: true, get: function () { return mongodb_1.Db; } });
Object.defineProperty(exports, "MongoClient", { enumerable: true, get: function () { return mongodb_1.MongoClient; } });
Object.defineProperty(exports, "ObjectId", { enumerable: true, get: function () { return mongodb_1.ObjectId; } });
class MongoConnect {
    name;
    emitter;
    mongoClient;
    client;
    userConfig;
    config;
    mode;
    reconnecting;
    healthyHosts;
    constructor(name, emitter, userConfig, mode) {
        this.name = name;
        this.emitter = emitter;
        this.userConfig = userConfig;
        this.config = {
            keepAlive: true,
            maxPoolSize: 5,
            connectTimeoutMS: 30000,
            socketTimeoutMS: 30000,
            serverSelectionTimeoutMS: 10000,
            readPreference: mongodb_1.ReadPreference.SECONDARY_PREFERRED,
        };
        this.config.authSource = (userConfig.auth || {}).authSource;
        this.mode = mode;
    }
    log(message, data) {
        this.emitter.emit("log", {
            service: this.name,
            message,
            data,
        });
    }
    success(message, data) {
        this.emitter.emit("success", {
            service: this.name,
            message,
            data,
        });
    }
    error(err, data) {
        this.emitter.emit("error", {
            service: this.name,
            data,
            err,
        });
    }
    getHealthyHosts() {
        return this.healthyHosts || [];
    }
    async getConnectionUrl() {
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
        joiner.push(servers.map((server) => `${server.host}:${server.port}`).join(","));
        const params = new URLSearchParams();
        if (this.name) {
            params.set("appName", this.name);
        }
        return joiner.join("") + (params.size > 0 ? "?" + params.toString() : "");
    }
    static isValidError(err) {
        return (err instanceof mongodb_1.MongoServerSelectionError ||
            err instanceof mongodb_1.MongoNetworkError ||
            err instanceof mongodb_1.MongoNetworkTimeoutError);
    }
    getClient() {
        return this.mongoClient;
    }
    async connect() {
        let connected = false;
        // Reconnection handler
        let attempt = 1;
        // Keep reference to old mongoClient, will need to close it later
        const oldMongoClient = this.mongoClient;
        while (!connected && attempt <= 10) {
            try {
                // Returns connection url with only healthy hosts
                const connectionUrl = await this.getConnectionUrl(); // C * 10 => 10C seconds
                const mongoClient = new mongodb_1.MongoClient(connectionUrl, {
                    ...this.config,
                    appName: this.name,
                }); // 10 * 10 => 100 seconds
                await mongoClient.connect();
                // Update this.mongoClient ONLY after a valid client has been established; else topology closed error will
                // be thrown will is not being monitored/is valid error for reconnection
                this.mongoClient = mongoClient;
                connected = true;
            }
            catch (err) {
                if (MongoConnect.isValidError(err)) {
                    this.error(err);
                    // 2 + 4 + 6 + 8 + 10 + 12 ... 20 => 2 * (1 + 2 + 3 + 4 ... 10) => 2 * ((10 * 11) / 2) => 110 seconds
                    await new Promise((res) => setTimeout(res, 2 * attempt * 1000));
                    attempt++;
                }
                else {
                    throw new Error(err);
                }
            }
        }
        this.client = this.mongoClient.db(this.userConfig.db);
        this.success(`Successfully connected in ${this.mode} mode`);
        if (oldMongoClient instanceof mongodb_1.MongoClient) {
            // Do NOT wait. If you wait, this might block indefinitely due to the older server being out of action.
            oldMongoClient.close();
        }
        return this;
    }
}
exports.MongoConnect = MongoConnect;
async function handleMongoError(err, mongo) {
    if (MongoConnect.isValidError(err)) {
        if (mongo.reconnecting === null) {
            mongo.reconnecting = mongo.connect()
                .then(() => {
                return null;
            });
        }
        await (mongo.reconnecting || Promise.resolve());
        mongo.reconnecting = null;
        return null;
    }
    return err;
}
var MODES;
(function (MODES) {
    MODES["SERVER"] = "server";
    MODES["REPLSET"] = "replset";
    MODES["SHARD"] = "shard";
})(MODES || (exports.MODES = MODES = {}));
function MongoFactory(mode, name, emitter, config) {
    switch (mode) {
        case MODES.SERVER:
            return new ServerMongo(name, emitter, config);
        case MODES.REPLSET:
            return new ReplSet(name, emitter, config);
        case MODES.SHARD:
            return new ShardMongo(name, emitter, config);
        default:
            throw new Error("Invalid architecture");
    }
}
class ServerMongo extends MongoConnect {
    constructor(name, emitter, config) {
        const { db, host, port, auth } = config;
        const userConfig = {
            db,
            getServers: () => Promise.resolve([{ host, port }]),
            auth,
        };
        super(name, emitter, userConfig, MODES.SERVER);
    }
}
class ReplSet extends MongoConnect {
    constructor(name, emitter, replicaConfig) {
        const { db, replica, auth } = replicaConfig;
        const config = {
            db: db,
            getServers: () => Promise.resolve(replica.servers),
            auth,
        };
        super(name, emitter, config, MODES.REPLSET);
        this.config.replicaSet = replica.name;
    }
}
class ShardMongo extends MongoConnect {
    constructor(name, emitter, shardConfig) {
        const { db, shard, auth } = shardConfig;
        super(name, emitter, { db, getServers: shard.getServers, auth }, MODES.SHARD);
    }
}
function MongoFactoryAuto(name, emitter, config) {
    if (config.replica) {
        return MongoFactory(MODES.REPLSET, name, emitter, config);
    }
    else if (config.shard) {
        return MongoFactory(MODES.SHARD, name, emitter, config);
    }
    else {
        return MongoFactory(MODES.SERVER, name, emitter, config);
    }
}
function isValidObjectId(value) {
    const regex = /[0-9a-f]{24}/;
    const matched = String(value).match(regex);
    if (!matched) {
        return false;
    }
    return mongodb_1.ObjectId.isValid(value);
}
function castToObjectId(value) {
    if (isValidObjectId(value) === false) {
        throw new TypeError(`Value passed is not valid objectId, is [ ${value} ]`);
    }
    return mongodb_1.ObjectId.createFromHexString(value);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiTW9uZ28uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvTW9uZ28udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7O0FBb0xBLDRDQWFDO0FBZ0NELG9DQWdCQztBQW9ERCw0Q0FRQztBQUVELDBDQVFDO0FBRUQsd0NBS0M7QUE3VEQscUNBU2lCO0FBd1RlLG1GQWhVOUIsWUFBRSxPQWdVOEI7QUFBZiw0RkEvVGpCLHFCQUFXLE9BK1RpQjtBQUFyQix5RkE3VFAsa0JBQVEsT0E2VE87QUEzUmpCLE1BQWEsWUFBWTtJQUN2QixJQUFJLENBQVM7SUFDYixPQUFPLENBQXNCO0lBQzdCLFdBQVcsQ0FBYztJQUN6QixNQUFNLENBQUs7SUFDWCxVQUFVLENBQWE7SUFDdkIsTUFBTSxDQUFxQjtJQUMzQixJQUFJLENBQVM7SUFDYixZQUFZLENBQWlCO0lBQ3JCLFlBQVksQ0FBVztJQUUvQixZQUNFLElBQVksRUFDWixPQUE0QixFQUM1QixVQUFzQixFQUN0QixJQUFZO1FBRVosSUFBSSxDQUFDLElBQUksR0FBRyxJQUFJLENBQUM7UUFDakIsSUFBSSxDQUFDLE9BQU8sR0FBRyxPQUFPLENBQUM7UUFDdkIsSUFBSSxDQUFDLFVBQVUsR0FBRyxVQUFVLENBQUM7UUFDN0IsSUFBSSxDQUFDLE1BQU0sR0FBRztZQUNaLFNBQVMsRUFBRSxJQUFJO1lBQ2YsV0FBVyxFQUFFLENBQUM7WUFDZCxnQkFBZ0IsRUFBRSxLQUFLO1lBQ3ZCLGVBQWUsRUFBRSxLQUFLO1lBQ3RCLHdCQUF3QixFQUFFLEtBQUs7WUFDL0IsY0FBYyxFQUFFLHdCQUFjLENBQUMsbUJBQW1CO1NBQ25ELENBQUM7UUFDRixJQUFJLENBQUMsTUFBTSxDQUFDLFVBQVUsR0FBRyxDQUFDLFVBQVUsQ0FBQyxJQUFJLElBQUksRUFBRSxDQUFDLENBQUMsVUFBVSxDQUFDO1FBQzVELElBQUksQ0FBQyxJQUFJLEdBQUcsSUFBSSxDQUFDO0lBQ25CLENBQUM7SUFFRCxHQUFHLENBQUMsT0FBZSxF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