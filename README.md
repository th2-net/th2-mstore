# Overview

Message store  (mstore) is an important th2 component which stores raw messages into Cradle. See [Cradle repository](https://github.com/th2-net/cradleapi/blob/master/README.md) for more detail. This component has a pin for listening messages via MQ. Users should mark a pin produced  raw messages in conn, read, hand boxes via the "store" attribute to automatically connect that pin to mstore and collect all messages into Cradle.

# Custom resources for infra-mgr

Infra schema can have only one mstore box description. It consists of one required option - docker image, pin configuration is generated and managed by infra-operator.
