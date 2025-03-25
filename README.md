# Overview (5.10.0)

Message store (mstore) is an important th2 component responsible for storing raw messages into Cradle. Please refer to [Cradle repository] (https://github.com/th2-net/cradleapi/blob/master/README.md) for more details. This component has a pin for listening messages via MQ.

## Protobuf raw message

Users must mark a pin that produces protobuf raw messages in conn, read and hand boxes via the "store" attribute, in order to automatically connect that pin to mstore and to collect all messages into Cradle.

Protobuf raw message is a base entity of th2. All incoming / outgoing data is stored in this format
Every protobuf raw message contains important parts:
* book - name of the book
* session alias - unique identifier of business session.
* session group - group id for this session
* direction - direction of message stream.
* sequence number - incremental identifier.
* data - byte representation of raw message 

book, session alias, direction and sequence number are a **compound unique identifier** of protobuf raw messages within th2

## Transport raw message

Users must connect a pin that produces transport raw messages in conn, read and hand boxes via the general link approach, in order to manually connect that pin to mstore and to collect all messages into Cradle.

Transport raw message is a new entity of th2. You can read more details about th2 transport protocol by the [link] (https://exactpro.atlassian.net/wiki/spaces/TH2/pages/1048838145/TH2+Transport+Protocol). All incoming / outgoing data is stored in this format
Every protobuf raw message contains important parts:
* book - name of the book
* session alias - unique identifier of business session.
* session group - group id for this session
* direction - direction of message stream.
* sequence number - incremental identifier.
* data - byte representation of raw message

book, session alias, direction and sequence number are a **compound unique identifier** of protobuf raw messages within th2

# Configuration

```json
{
  "maxTaskCount" : 256,
  "maxTaskDataSize" : 256000000,
  "maxRetryCount" : 5,
  "retryDelayBase" : 5000,
  
  "rebatching" : true,
  "drain-interval" : 100,
  "prefetchRatioToDrain" : 0.8,
  "maxBatchSize" : 200000,
  "persisotr-termination-timeout" : 1000,
  "termination-timeout" : 1000
}
```
+ _maxTaskCount_ - Maximum number of message batches that will be processed simultaneously
+ _maxTaskDataSize_ - Maximum total data size of messages during parallel processing
+ _maxRetryCount_ - Maximum number of retries that will be done in case of message batch persistence failure
+ _retryDelayBase_ - Constant that will be used to calculate next retry time(ms):
  retryDelayBase * retryNumber
+ _rebatching_ - Parameter determining if mstore should perform re-batching on incoming batches to avoid writing small batches
+ _drain-interval_ - Interval in milliseconds to drain all aggregated batches that are not stored yet. The default value is 1000.
+ _prefetchRatioToDrain_ - Threshold ratio of fetched messages to RabbitMQ prefetch size to force aggregated batch draining.
+ _maxBatchSize_ - Maximum aggregated batch size in case of re-batching
+ _termination-timeout_ - Timeout in milliseconds to await for the inner drain scheduler to finish all the tasks. The default value is 5000.
+ _persisotr-termination-timeout_ - The timeout in milliseconds to await for the persisotr thread complete. The default value is 5000.

If some of these parameters are not provided, mstore will use default(undocumented) value.
If _maxTaskCount_ or _maxTaskDataSize_ limits are reached during processing, mstore will pause processing new messages
until some events are processed


# Custom resources for infra-mgr

Infra schema can only contain one mstore box description. It consists of one required option - docker image . Pin configuration is generated and managed by infra-operator.

### Quick start
General view of the component will look like this:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Mstore
metadata:
  name: mstore
spec:
  image-name: ghcr.io/th2-net/th2-mstore
  image-version: <image version>
  custom-settings:
    maxTaskCount: 256
    maxTaskDataSize: 256000000
    maxRetryCount: 5
    retryDelayBase: 5000
    rebatching: true
    drain-interval: 100
    prefetchRatioToDrain: 0.8
    maxBatchSize: 200000
    termination-timeout: 5000
    persisotr-termination-timeout: 5000
  pins:
    mq:
      subscribers:
      - name: transport
        attributes:
        - transport-group
        - subscribe
  extended-settings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError -Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\""
    resources:
      limits:
        memory: 1024Mi
        cpu: 2000m
      requests:
        memory: 512Mi
        cpu: 1000m
```

# Common features

This is a list of supported features provided by libraries.
Please see more details about this feature via [link](https://github.com/th2-net/th2-common-j#configuration-formats).

# Release notes

## next
* Enabled [Cassandra driver metrics](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/metrics/index.html)

## 5.10.0
* Publish mstore as moven artifact.
* Prepared MessagePersistor class for using in other project.
* Added `persisotr-termination-timeout` option
* Updated
    * th2 gradle plugin `0.2.4` based on bom: `4.11.0`
    * task-utils: `0.1.3`

## 5.9.0
* Updated
  * th2 gradle plugin `0.1.6` based on bom: `4.9.0`
  * cradle api: `5.4.4-dev`
  * common-utils: `2.3.0-dev`

## 5.8.0
* Updated th2 gradle plugin `0.1.1`
* Updated common: `5.14.0-dev`
* Updated cradle api: `5.4.1-dev`

## 5.7.0

* Migrated to th2 gradle plugin `0.0.8`
* Updated common: `5.12.0-dev`
* Updated common-utils: `2.2.3-dev`

## 5.6.0

* Migrated to th2 gradle plugin `0.0.6` 
* Updated bom: `4.6.1`
* Updated common: `5.11.0-dev`
* Updated cradle api: `5.3.0-dev`

## 5.5.0

* Updated bom: `4.6.0` 
* Updated common: `5.9.1-dev` 

## 5.4.0

* Updated cradle api: `5.2.0-dev`
* Updated common: `5.8.0-dev`

## 5.3.1

* Add information about group and book into error message during batch processing

## 5.3.0

* Mstore publishes event with aggregated statistics about internal errors into event router periodically 
* Updated common: `5.6.0-dev`
* Added common-utils: `2.2.2-dev`

## 5.2.4

* Migrated to the cradle version with fixed load pages where `removed` field is null problem.
* Updated cradle: `5.1.4-dev`

## 5.2.3

* Reverted cradle: `5.1.1-dev` because `5.1.3-dev` can't work with pages where `removed` field is null.

## 5.2.2

* Fixed the lost messages problem when mstore is restarted under a load 
* Updated common: `5.4.1-dev`
* Updated cradle: `5.1.3-dev`

## 5.2.1

* Fixed the problem: batches for the same session group and different books are separate storing

## 5.2.0
* Updated bom: `4.5.0-dev`
* Updated common: `5.4.0-dev`

## 5.1.0
+ Provided ability to process th2 transport messages

## 5.0.1

+ Stores message properties to cradle 

## 5.0.0
+ Migration to books/pages cradle 5.0.0

## 3.4.1

+ Added util methods from store-common
+ Removed dependency to store-common

## 3.4.0

+ Update common version from `3.18.0` to `3.29.0`
+ Update store-common version from `3.1.0` to `3.2.0`

## 3.3.0

### Changed:

+ Disable waiting for connection recovery when closing the `SubscribeMonitor`
+ Update Cradle version from `2.9.1` to `2.13.0`
+ Rework logging for incoming and outgoing messages
+ Resets embedded log4j configuration before configuring it from a file

## 3.1.0

+ Compressed metadata for events
+ Async API for storing messages