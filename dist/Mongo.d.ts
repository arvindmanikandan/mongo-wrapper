import events from "events";
import { Db, MongoClient, MongoClientOptions, ObjectId, MongoServerSelectionError, MongoNetworkError, MongoTimeoutError } from "mongodb";
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
export declare class MongoConnect implements Mongo {
    name: string;
    emitter: events.EventEmitter;
    mongoClient: MongoClient;
    client: Db;
    userConfig: UserConfig;
    config: MongoClientOptions;
    mode: string;
    reconnecting: Promise<Mongo>;
    private healthyHosts;
    constructor(name: string, emitter: events.EventEmitter, userConfig: UserConfig, mode: string);
    log(message: string, data?: Record<string, any>): void;
    success(message: string, data?: Record<string, any>): void;
    error(err: Error, data?: Record<string, any>): void;
    getHealthyHosts(): Server[];
    private getConnectionUrl;
    static isValidError(err: Error): err is MongoServerSelectionError | MongoTimeoutError | MongoNetworkError;
    getClient(): MongoClient;
    connect(): Promise<Mongo>;
}
export declare function handleMongoError(err: Error, mongo: Mongo): Promise<Error>;
export declare enum MODES {
    SERVER = "server",
    REPLSET = "replset",
    SHARD = "shard"
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
export declare function MongoFactory(mode: MODES, name: string, emitter: events.EventEmitter, config: ServerConfig | ReplicaConfig | ShardConfig): Mongo;
export declare function MongoFactoryAuto(name: string, emitter: events.EventEmitter, config: MongoConfig): Mongo;
export declare function isValidObjectId(value: string | number | ObjectId): boolean;
export declare function castToObjectId(value: string): ObjectId;
export type MongoConfig = ServerConfig | ReplicaConfig | ShardConfig;
export { ObjectId, MongoClient, Db };
