# Overview (4.1.1)

Message store (mstore) is an important th2 component responsible for storing raw messages into Cradle. Please refer to [Cradle repository] (https://github.com/th2-net/cradleapi/blob/master/README.md) for more details. This component has a pin for listening messages via MQ.

Users must mark a pin that produces raw messages in conn, read and hand boxes via the "store" attribute, in order to automatically connect that pin to mstore and to collect all messages into Cradle.

Raw message is a base entity of th2. All incoming / outgoing data is stored in this format
Every raw message contains important parts:
* session alias - unique identifier of business session.
* direction - direction of message stream.
* sequence number - incremental identifier.
* data - byte representation of raw message 

session alias, direction and sequence number are a **compound unique identifier** of raw messages within th2

# Configuration

```json
{
  "drain-interval": 1000,
  "termination-timeout": 5000,
  "maxTaskCount" : 256,
  "maxTaskDataSize" : 133169152,
  "maxRetryCount" : 1000000,
  "retryDelayBase" : 5000
}
```

+ _drain-interval_ - Interval in milliseconds to drain all aggregated batches that are not stored yet. The default value is 1000.
+ _termination-timeout_ - Timeout in milliseconds to await for the inner drain scheduler to finish all the tasks. The default value is 5000.
+ _maxTaskCount_ - Maximum number of message batches that will be processed simultaneously
+ _maxTaskDataSize_ - Maximum total data size of messages during parallel processing
+ _maxRetryCount_ - Maximum number of retries that will be done in case of message batch persistence failure
+ _retryDelayBase_ - Constant that will be used to calculate next retry time(ms):
  retryDelayBase * retryNumber

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
    drain-interval: 1000
    termination-timeout: 5000
  extended-settings:
    service:
      enabled: false
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError -Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\""
    resources:
      limits:
        memory: 500Mi
        cpu: 200m
      requests:
        memory: 100Mi
        cpu: 20m
```

# Common features

This is a list of supported features provided by libraries.
Please see more details about this feature via [link](https://github.com/th2-net/th2-common-j#configuration-formats).

## 4.1.1

+ Using Cradle 3.1.4. No changes related to message persistence
+ Changed default configuration to
```json
{
    "maxTaskCount" : 256,
    "maxRetryCount" : 1000000,
    "retryDelayBase" : 5000
}
```
## 4.1.0
+ Added metrics collection for Prometheus
+ Limiting simultaneously processed message batches by number and content size
+ Retrying storing message batches in case of failure
+ To check message batch ordering mstore now loads on startup last sequence number and timestamp from cradle
+ Updated cradle version from `3.1.1` to `3.1.3` **data migration is necessary from previous version**

## 4.0.0

+ Update common version from `3.35.0` to `3.39.3`
+ Update Cradle version from `2.21.0` to `3.1.1`. **Note, that the new Cradle version is not compatible with a previous version.**
  **The data migration is required in order to keep the current data and migrate to new Cradle version**

## 3.6.0

+ Update common version from `3.31.6` to `3.35.0`
+ Update Cradle version from `2.20.2` to `2.21.0`
+ Added more detail into logs
+ Added checking for timestamp inside batch and between batches

## 3.5.0

+ Update common version from `3.30.0` to `3.31.6`
+ Update Cradle version from `2.20.0` to `2.20.2`

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