import debug from './debug'
import { Schema, SchemaOptions, SchemaProperties, SchemaValidationOptions } from 'tdv'
import { ValidationResult } from 'joi'
import AWS from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { enumerable } from './decorator'
import { Hook } from './hook'
import { Put } from './put'
import { Get } from './get'
import { Query } from './query'
import { Scan } from './scan'
import { Update } from './update'
import { Delete } from './delete'
import { BatchGet } from './batchGet'
import { BatchWrite } from './batchWrite'
import './Symbol.asyncIterator'

const log = debug('model')

export const $put = Symbol.for('put')
export const $get = Symbol.for('get')
export const $query = Symbol.for('query')
export const $scan = Symbol.for('scan')
export const $update = Symbol.for('update')
export const $delete = Symbol.for('delete')
export const $batchGet = Symbol.for('batchGet')
export const $batchWrite = Symbol.for('batchWrite')

const { NODE_ENV } = process.env
const RETURN_CONSUMED_CAPACITY = NODE_ENV === 'production' ? 'NONE' : 'TOTAL'
const RETURN_ITEM_COLLECTION_METRICS = NODE_ENV === 'production' ? 'NONE' : 'SIZE'

export class Model extends Schema {
    /**
     * AWS reference
     */
    static AWS = AWS

    // protected static _ddb: DynamoDB
    /**
     * AWS DynamoDB instance
     */
    // static get ddb() {
    //     return this._ddb
    //         || (this._ddb = new DynamoDB())
    // }
    static ddb = new AWS.DynamoDB()

    // protected static _documentClient: DocumentClient
    /**
     * AWS DynamoDB DocumentClient instance
     */
    // static get client() {
    //     return this._documentClient
    //         || (this._documentClient = new DocumentClient())
    // }
    static client = new DocumentClient()

    /**
     * Configure tiamo to use a DynamoDB local endpoint for testing.
     * 
     * @param endpoint defaults to 'http://localhost:4567'
     */
    static local(endpoint = 'http://localhost:4567') {
        this.ddb = new AWS.DynamoDB({ endpoint })
        this.client = new DocumentClient({ service: this.ddb })
    }

    /**
     * Table name store in metadata
     */
    static get tableName(): string {
        return Reflect.getOwnMetadata('tiamo:table:name', this) || this.name
    }

    /**
     * HASH key store in metadata
     */
    static get hashKey() {
        const key = Reflect.getOwnMetadata('tiamo:table:hash', this.prototype)

        if (!key) throw new Error(`Model ${this.name} missing hash key`)

        return key
    }

    /**
     * RANGE key store in metadata
     */
    static get rangeKey() {
        return Reflect.getOwnMetadata('tiamo:table:range', this.prototype)
    }

    /**
     * Global indexes definition store in metadata
     */
    static get globalIndexes() {
        return this.getIndexes()
    }

    /**
     * Local indexes definition store in metadata
     */
    static get localIndexes() {
        return this.getIndexes('local')
    }

    private static getIndexes(scope: 'global' | 'local' = 'global') {
        const cacheKey = `tiamo:cache:${scope}Indexes`

        if (Reflect.hasOwnMetadata(cacheKey, this)) {
            return Reflect.getOwnMetadata(cacheKey, this)
        }

        const indexes = (Reflect.getMetadataKeys(this.prototype) as string[])
            .reduce((res, key) => {
                if (!key.startsWith(`tiamo:table:index:${scope}:`)) return res

                const [type, name] = key.split(':').reverse()

                res[name] = res[name] || {}
                res[name][type] = Reflect.getMetadata(key, this.prototype)

                return res
            }, {})

        Reflect.defineMetadata(cacheKey, indexes, this)

        return indexes
    }

    /**
     * Hook singleton instance
     */
    // protected static get hook() {
    //     const modelMeta = modelMetaFor(this.prototype)

    //     return modelMeta.hook = modelMeta.hook || new Hook()
    // }
    protected static hook = new Hook()

    /**
     * Timestamps definition store in metadata
     */

    static get timestamps() {
        const cacheKey = 'tiamo:cache:timestamps'

        if (Reflect.hasOwnMetadata(cacheKey, this)) {
            return Reflect.getOwnMetadata(cacheKey, this)
        }

        const timestamps = {}
        
        for (let type of ['create', 'update', 'expire']) {
            const key = `tiamo:timestamp:${type}`
            if (Reflect.hasMetadata(key, this.prototype)) {
                timestamps[type] = Reflect.getMetadata(key, this.prototype)
            }
        }

        Reflect.defineMetadata(cacheKey, timestamps, this)
        
        return timestamps
    }

    /**
     * Create model instance. Build and put but not overwrite existed one.
     */
    static async create<M extends Model>(this: ModelStatic<M>, props: SchemaProperties<M>, options = {}) {
        const Item = (this.build(props, { convert: false }) as M).validate({ raise: true }).value

        const put = this.put(Item).where(this.hashKey).not.exists()
        if (this.rangeKey) put.where(this.rangeKey).not.exists()

        return put
    }

    /**
     * Put item into db
     */
    static put<M extends Model>(this: ModelStatic<M>, Item: DocumentClient.PutItemInputAttributeMap) {
        return new Put<M>({ Model: this, Item })
    }

    /**
     * Get item by key
     */
    static get<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Get<M>({ Model: this, Key })
    }

    /**
     * Query items by key
     */
    static query<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M[]>({ Model: this }),
        )
    }

    /**
     * Scan items
     */
    static scan<M extends Model>(this: ModelStatic<M>) {
        return new Scan<M>({ Model: this })
    }

    /**
     * Update item by key
     */
    static update<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Update<M>({ Model: this, Key })
    }

    /**
     * Delete item by key
     */
    static delete<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Delete<M>({ Model: this, Key })
    }

    /**
     * Batch operate
     * 
     * * Chain call `put` and `delete`
     * * One way switch context from `put` or `delete` to `get`
     * * Operate order `put` -> `delete` -> `get` -> return
     * 
     * @return PromiseLike or AsyncIterable
     * 
     * @example
     * 
     *      // get only
     *      Model.batch().get({})
     *      // write only
     *      Model.batch().put({})
     *      // chain
     *      Model.batch().put({}).delete({}).get({})
     *      // async interator
     *      for await (let m of Model.batch().get([])) {
     *          console.log(m.id)
     *      }
     */
    static batch<M extends Model>(this: ModelStatic<M>) {
        const self = this

        return {
            /**
             * Batch get
             * 
             * @example
             * 
             *      Model.batch().get({ id: 1 })
             */
            get(...GetKeys: DocumentClient.KeyList) {
                return new BatchGet<M>({ Model: self, GetKeys })
            },
            /**
             * Batch write put request
             * 
             * @example
             * 
             *      Model.batch().put({ id: 1, name: 'tiamo' })
             */
            put(...PutItems: DocumentClient.PutItemInputAttributeMap[]) {
                return new BatchWrite<M>({ Model: self, PutItems })
            },
            /**
             * Batch write delete request
             * 
             * @example
             * 
             *      Model.batch().delete({ id: 1 })
             */
            delete(...DeleteKeys: DocumentClient.KeyList) {
                return new BatchWrite<M>({ Model: self, DeleteKeys })
            },
        }
    }

    static [$batchGet] = async function* (this: ModelStatic<Model>, params: DocumentClient.BatchGetItemInput) {
        let { RequestItems, ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY } = params
        let i = 0

        do {
            i++
            const p = { RequestItems, ReturnConsumedCapacity }

            log('⇡ [BATCHGET]#%d request params: %o', i, Object.keys(p.RequestItems).reduce((a, k) => {
                a[k] = p.RequestItems[k].Keys.length
                return a
            }, {}))

            let res = await this.client.batchGet(p).promise()
            if (res.ConsumedCapacity) log('⇣ [BATCHGET]#%d consumed capacity:', i, res.ConsumedCapacity)

            yield res.Responses

            RequestItems = res.UnprocessedKeys
        } while (Object.keys(RequestItems).length) // last time is {}
    }

    static [$batchWrite] = async function* (this: ModelStatic<Model>, params: DocumentClient.BatchWriteItemInput) {
        let {
            RequestItems,
            ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY,
            ReturnItemCollectionMetrics = RETURN_ITEM_COLLECTION_METRICS,
        } = params
        let i = 0

        do {
            i++
            const p = { RequestItems, ReturnConsumedCapacity, ReturnItemCollectionMetrics }

            log('⇡ [BATCHWRITE][%d] request...', i)

            let res = await this.client.batchWrite(p).promise()
            if (res.ConsumedCapacity) log('⇣ [BATCHWRITE][%d] consumed capacity:', i, res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [BATCHWRITE][%d] item collection metrics:', i, res.ItemCollectionMetrics)

            yield // what yield from batchWrite?

            RequestItems = res.UnprocessedItems
        } while (Object.keys(RequestItems).length) // last time is {}
    }

    static [$put](params: Partial<DocumentClient.PutItemInput>) {
        const p = { ...params } as DocumentClient.PutItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_OLD'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [PUT] request params:', p)

        return this.client.put(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [PUT] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [PUT] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    static [$get](params: Partial<DocumentClient.GetItemInput>) {
        const p = { ...params } as DocumentClient.GetItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY

        log('⇡ [GET] request params:', p)

        return this.client.get(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [GET] consumed capacity: ', res.ConsumedCapacity)
            return res.Item
        })
    }

    static [$query] = async function* (this: ModelStatic<Model>, params: Partial<DocumentClient.QueryInput>) {
        let ExclusiveStartKey
        let { ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY, ...other } = params
        let i = 0

        do {
            i++
            const p = { ...other, ExclusiveStartKey, ReturnConsumedCapacity } as DocumentClient.QueryInput
            p.TableName = p.TableName || this.tableName

            log('⇡ [QUERY]#%d request params: %o', i, p)

            let res = await this.client.query(p).promise()
            if (res.ConsumedCapacity) log('⇣ [QUERY]#%d consumed capacity:', i, res.ConsumedCapacity)

            if (p.Select === 'COUNT') {
                yield res.Count
            } else {
                yield res.Items
            }

            ExclusiveStartKey = res.LastEvaluatedKey
        } while (i == 0 || ExclusiveStartKey)
    }

    static [$scan] = async function* (this: ModelStatic<Model>, params: Partial<DocumentClient.ScanInput>) {
        let ExclusiveStartKey
        let { ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY, ...other } = params
        let i = 0

        do {
            i++
            const p = { ...other, ExclusiveStartKey, ReturnConsumedCapacity } as DocumentClient.ScanInput
            p.TableName = p.TableName || this.tableName

            log('⇡ [SCAN]#%d request params: %o', i, p)

            let res = await this.client.scan(p).promise()
            if (res.ConsumedCapacity) log('⇣ [SCAN]#%d consumed capacity:', i, res.ConsumedCapacity)

            if (p.Select === 'COUNT') {
                yield res.Count
            } else {
                yield res.Items
            }

            ExclusiveStartKey = res.LastEvaluatedKey
        } while (i == 0 || ExclusiveStartKey)
    }

    static [$update](params: Partial<DocumentClient.UpdateItemInput>) {
        const p = { ...params } as DocumentClient.UpdateItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_NEW'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [UPDATE] request params:', p)
        return this.client.update(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [UPDATE] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [UPDATE] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    static [$delete](params: Partial<DocumentClient.DeleteItemInput>) {
        const p = { ...params } as DocumentClient.DeleteItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_OLD'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [DELETE] request params:', p)

        return this.client.delete(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [DELETE] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [DELETE] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    /**
     * @see https://github.com/Microsoft/TypeScript/issues/3841#issuecomment-337560146
     */
    ['constructor']: ModelStatic<this>

    constructor(props?, options?: SchemaOptions) {
        super(props, options)
    }

    /**
     * Pre hook
     */
    pre(name: string, fn: Function) {
        this.constructor.hook.pre(name, fn)

        return this
    }

    /**
     * Post hook
     */
    post(name: string, fn: Function) {
        this.constructor.hook.post(name, fn)

        return this
    }

    /**
     * Validate by Joi
     * 
     * * Be careful the value returned is a new instance. This is design by Joi.
     * * We strip unknown properties so you can put your fields safely.
     */
    validate(options = {} as SchemaValidationOptions) {
        return this._validate(options)
    }
    // @enumerable(false)
    get _validate() {
        return this.constructor.hook.wrap('validate', (options = {} as SchemaValidationOptions) => {
            return super.validate({
                // when true, ignores unknown keys with a function value. Defaults to false.
                skipFunctions: true,
                // when true, all unknown elements will be removed.
                stripUnknown: true,
                ...options,
            })
        })
    }
    // set _validate(v) { }

    /**
     * Save into db
     * 
     * Validate before save. Throw `ValidateError` if invalid. Apply casting and default if valid.
     */
    async save(options?) {
        return this.constructor.put({
            Item: this.validate({ apply: true, raise: true }).value,
        }).then(() => this)
    }

    /**
     * Delete from db by key
     */
    delete(options?) {
        const { hashKey, rangeKey } = this.constructor
        const Key: DocumentClient.Key = { [hashKey]: this[hashKey] }
        if (rangeKey) Key[rangeKey] = this[rangeKey]
        return this.constructor[$delete]({ Key })
    }
}

/* TYPES */

/**
 * Model static method this type
 * This hack make sub class static method return sub instance
 * But break IntelliSense autocomplete in Typescript@2.7
 * 
 * @example
 * 
 *      static method<M extends Class>(this: ModelStatic<M>)
 * 
 * @see https://github.com/Microsoft/TypeScript/issues/5863#issuecomment-302891200
 */
export type ModelStatic<T> = typeof Model & {
    new(...args): T
    // get(...args): any // IntelliSense still not work :(
}