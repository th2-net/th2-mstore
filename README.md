# Overview

Message store  (mstore) is an important th2 component which stores raw messages into Cradle. See [Cradle repository](https://github.com/th2-net/cradleapi/blob/master/README.md) for more detail. This component has a pin for listening messages via MQ. 

Users should mark a pin produced  raw messages in conn, read, hand boxes via the "store" attribute to automatically connect that pin to mstore and collect all messages into Cradle.

Raw message is a base entity of th2. All ecumming / outgoing data are stored in this format
Every raw message has got important parts:
* session alias - unique identifier of business session.
* direction - direction of message stream.
* sequence number - incremental identifier.
* data - byte representation of raw message 

session alias, direction and sequence sequence number is a composite unique identifier of raw message within th2

# Custom resources for infra-mgr

Infra schema can have only one mstore box description. It consists of one required option - docker image, pin configuration is generated and managed by infra-operator.
