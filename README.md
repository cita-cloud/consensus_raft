# consensus_raft
[![Build Status](https://travis-ci.org/cita-cloud/consensus_raft.svg?branch=master)](https://travis-ci.org/cita-cloud/consensus_raft)

The raft consensus component for CITA Cloud.

It uses [raft-rs](https://github.com/tikv/raft-rs) as the raft implementation.

## Usage
```
cargo build
```

You may find more details in [runner_k8s](https://github.com/cita-cloud/runner_k8s) and [runner_consul](https://github.com/cita-cloud/runner_consul)

## Build docker image
```
docker build -t citacloud/consensus_raft .
```
