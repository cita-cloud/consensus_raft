# consensus_raft
[![Build Status](https://travis-ci.org/cita-cloud/consensus_raft.svg?branch=master)](https://travis-ci.org/cita-cloud/consensus_raft)

The raft consensus component for CITA Cloud.

It uses [raft-rs](https://github.com/tikv/raft-rs) as the raft implementation.

## Usage
```
cargo build --release
```

You may find more details in [runner_k8s](https://github.com/cita-cloud/runner_k8s) and [runner_consul](https://github.com/cita-cloud/runner_consul)

## Build docker image
```
docker build -t citacloud/consensus_raft .
```

## Build your own consensus service

Please check the [consensus.proto](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/consensus.proto)
in [cita_cloud_proto](https://github.com/cita-cloud/cita_cloud_proto)
which defines the service that consensus should implement.

To communicate with other peers, you need to:
1. Implement the [`NetworkMsgHandlerService`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L39)
which handles the message from peers.
2. Register your service to the network by [`RegisterNetworkMsgHandler`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L35),
which tells the network to forward the messages you are concerned about.

After all of that, you can send your messages to others by [`SendMsg`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L26) 
or [`Broadcast`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L29) provided by the network service.
