/// <reference types="node"/>

declare module 'dynogels' {
    import * as Joi from 'joi';
    import {CreateTableInput, CreateTableOutput, UpdateTableOutput, DescribeTableOutput, DeleteTableOutput} from 'aws-sdk/clients/dynamodb';
    import * as DynamoDB from 'aws-sdk/clients/dynamodb';

    export type Callback<T> = (err: any | null, data: T) => void;

    export interface Logger {
        info(base: string, ...args: any[]): void;
        warn(errDesc: { err: object }, base: string, ...args: any[]): void;
    }

    export interface LocalSecondaryIndexSchema {
        type: 'local';
        hashKey: string;
        rangeKey: string;
        name: string;
        projection: any;
    }
    
    export interface GlobalSecondaryIndexSchema {
        type: 'global';
        hashKey: string;
        rangeKey?: string;
        name: string;
        projection?: any;
        readCapacity?: number;
        writeCapacity?: number;
    }

    export type SecondaryIndexSchema = GlobalSecondaryIndexSchema | LocalSecondaryIndexSchema;

    export interface ConfigSchema {
        hashKey: string;
        rangeKey?: string;
        tableName?: string | (() => string);
        indexes?: SecondaryIndexSchema[];
        schema?: object;
        timestamps?: boolean;
        createdAt?: string | boolean;
        updatedAt?: string | boolean;
        log?: Logger
    }

    export interface KeyCondition {
        equals(eq: any): Query;
        eq(eq: any): Query;
        lte(v: any): Query;
        lt(v: any): Query;
        gte(v: any): Query;
        gt(v: any): Query;
        beginsWith(v: any): Query;
        between(v: any): Query;
    }

    export interface FilterCondition {
        equals(eq: any): Query;
        eq(eq: any): Query;
        lte(v: any): Query;
        lt(v: any): Query;
        gte(v: any): Query;
        gt(v: any): Query;
        beginsWith(v: any): Query;
        between(v: any): Query;

        null(): Query;
        exists(): Query;
        contains(v: any): Query;
        notContains(v: any): Query;
        in(v: any): Query;
    }
    
    export interface Query {
        limit(num: number): this;
        filterExpression(expression: string): this;
        expressionAttributeValues(data: any): this;
        expressionAttributeNames(data: any): this;
        projectionExpression(data: any): this;
        usingIndex(name: string): this;
        consistentRead(read: boolean): this;
        addKeyCondition(condition: any): this;
        addFilterCondition(condition: any): this;
        startKey(hashKey: any, rangeKey: any): this;
        attributes(attrs: any): this;
        ascending(): this;
        descending(): this;
        select(value: any): this;
        returnConsumedCapacity(value?: any): this;
        loadAll(): this;
        where(keyName: any): KeyCondition;
        filter(keyName: any): FilterCondition;
        exec(callback: Callback<any>): void;
    }

    export interface Scan {
        limit(num: number): this;
        addFilterCondition(condition: any): this;
        startKey(hashKey: any, rangeKey?: any): this;
        attributes(attrs: any): this;
        select(value: any): this;
        returnConsumedCapacity(value?: any): this;
        segments(segment: any, totalSegments: any): this;
        where(keyName: any): FilterCondition;
        filterExpression(expression: string): this;
        expressionAttributeValues(data: any): this;
        expressionAttributeNames(data: any): this;
        projectionExpression(data: any): this;
        exec(callback: Callback<any>): void;
        loadAll(): this;
    }

    type KeyValue = number | string | Buffer;

    type Item = {
        [attr: string]: any;
    };

    export interface Model {
        tableName(): string;

        get(hashKey: KeyValue, rangeKey: KeyValue, callback: Callback<Item>): void;
        get(hashKey: KeyValue, rangeKey: KeyValue, options: any, callback: Callback<Item>): void;
        get(keys: any, callback: Callback<Item>): void;
        get(keys: any, options: any, callback: Callback<Item>): void;

        create(item: Item[], callback: Callback<Item[]>): void;
        create(item: Item[], options: any, callback: Callback<Item[]>): void;
        create(item: Item, callback: Callback<Item>): void;
        create(item: Item, options: any, callback: Callback<Item>): void;

        update(item: Item, callback: Callback<Item>): void;
        update(item: Item, options: any, callback: Callback<Item>): void;

        destroy(hashKey: KeyValue, rangeKey: KeyValue, callback: Callback<Item>): void;
        destroy(hashKey: KeyValue, rangeKey: KeyValue, options: any, callback: Callback<Item>): void;
        destroy(keys: any, callback: Callback<Item>): void;
        destroy(keys: any, options: any, callback: Callback<Item>): void;

        query(hashKey: KeyValue): Query;

        scan(): Scan;
        parallelScan(): Scan;

        getItems(keys: (Item | KeyValue)[], callback: Callback<Item[]>): void;
        batchGetItems(keys: (Item | KeyValue)[], callback: Callback<Item[]>): void;

        createTable(callback: Callback<CreateTableOutput>): void;
        createTable(options: Partial<CreateTableInput>, callback: Callback<CreateTableOutput>): void;

        updateTable(callback: Callback<UpdateTableOutput>): void;
        updateTable(throughput: any, callback: Callback<UpdateTableOutput>): void;

        describeTable(callback: Callback<DescribeTableOutput>): void;

        deleteTable(callback: Callback<DeleteTableOutput>): void;

        readonly log: Logger;

        after(type: 'create' | 'update' | 'destroy', handler: (item: Item) => void): void;
        before(type: 'create' | 'update', handler: (item: Item, callback: Callback<Item>) => void): void;

        readonly docClient: DynamoDB;
        readonly schema: object;
        config(config: { tableName?: string, docClient?: DynamoDB, dynamodb?: DynamoDB}): { readonly name: string };
    }

    export function define(modelName: string, config: ConfigSchema): Table;   

    export namespace types {
        function stringSet(): Joi.AnySchema;
        function numberSet(): Joi.AnySchema;
        function uuid(): Joi.AnySchema;
        function timeUUID(): Joi.AnySchema;
    }
}