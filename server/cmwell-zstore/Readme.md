# zStore

zStore is a library CM-Well uses in several modules, to keep generic key-value data in a persistent (optionally temporal), distributed manner. Underlying implementation is Cassandra.

## Usages
CM-Well uses zStore for several tasks, such as
- Keeping large FileInfotons
- Temporarily store large payload of a large Infoton during ingest: while the meta-data is delivered from WS to BG through Kafka, if the payload is too large it is redirected to zStore and referenced from Kafka, for the BG to populate it once consumed that Kafka message.
- TrackingActors persistence
- STP progress persistence
- BG/DCC progress persistence 

## Features

### Self contained CQL setup
Besides `path` and `infoton` tables CM-Well has, there's another `zstore` table in the "data2" keyspace. It's columns are `uzid`, `field` and `value` where the latter is blob. zStore object runs the `CREATE IF NOT EXISTS` CQL statement when used from any module. 

> Note: Cassandra recommends against running multiple "CREATE IF NOT EXISTS" statements on the same table in parallel. That can lead to schema version errors. Because of that, CM-Well initialization contains the zstore table creation, so it will be created when a cluster is created. Nonetheless, we keep the CQL statements self-contained in zStore object so it can be reused in other projects.   

Part of zStore logic is to serialize / deserialize large binary values into smaller chunks in zStore table. 

### Simple put/get API
zStore provides a very simple API of put and get methods where the key is `String` and the value is `Array[Byte]`. Since this involves IO, all results are Futures. It also provides an optional TTL parameter for the put method for cases when the user wants to have the value persisted only for a certain period of time. This facilitates the built-in TTL feature in Cassandra. Please refer to the `ZStore` trait in code to see all APIs. 


### Caching layer: zCache
On top of core `ZStore` API, there's a caching layer that uses the TTL API of zStore, and provides the following:
- Type Parameters: User can use any type for their keys and values, as long as they provide some auxiliary methods of serialization. Future improvement may be to use Type Classes here.
- Memoize method: The caching is served as a wrapping `memoize( ... )` method for user's original methods - so no refactoring needs to be done in original user code (as the memoize return the same signature as its input).
- L1/L2: The most recommended flavour of zCache is L1L2 method which has an in-memory cache as a first layer, and then zStore is used.    

## zz API
WS provides an easy way to interact with zStore via HTTP. Syntax and examples can be found in the live help page: Please use HTTP GET for `cmwell/zz` on any running CM-Well cluster.