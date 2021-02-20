# Overview (2.5.0)

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
  "termination-timeout": 5000
}
```

#### drain-interval
Interval in milliseconds to drain all aggregated batches that are not stored yet. The default value is 1000.

#### termination-timeout
The timeout in milliseconds to await for the inner drain scheduler to finish all the tasks. The default value is 5000.

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
1. CradleMaxMessageBatchSize - this option defines the maximum message batch size in bytes.
   Please see more details about this feature via [link](https://github.com/th2-net/th2-common-j#configuration-formats)
