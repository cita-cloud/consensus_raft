# consensus_raft
[![Build Status](https://travis-ci.org/cita-cloud/consensus_raft.svg?branch=master)](https://travis-ci.org/cita-cloud/consensus_raft)

`CITA-Cloud`中[consensus微服务](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/consensus.proto)的实现，基于[raft-rs](https://github.com/tikv/raft-rs)。

## 编译docker镜像
```
docker build -t citacloud/consensus_raft .
```

## 使用方法

```
$ consensus -h
consensus 6.6.4
Rivtower Technologies <contact@rivtower.com>

Usage: consensus [COMMAND]

Commands:
  run   run the service
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### consensus-run

运行`consensus`服务。

```
$ consensus run -h
consensus-run
run the service

USAGE:
    consensus run [OPTIONS]

OPTIONS:
    -c, --config <config>                  the consensus config [default: config.toml]
    -d, --log-dir <log-dir>                the log dir. Overrides the config
    -f, --log-file-name <log-file-name>    the log file name. Overrride the config
    -h, --help                             Print help information
        --stdout                           if specified, log to stdout. Overrides the config
```

参数：
1. `config` 微服务配置文件。

    参见示例`example/config.toml`。

    其中：
    * `controller_port` 为依赖的`controller`微服务的`gRPC`服务监听的端口号。
    * `grpc_listen_port` 为本微服务`gRPC`服务监听的端口号。
    * `network_port` 为依赖的`network`微服务的`gRPC`服务监听的端口号。
    * `node_addr` 为本节点地址文件路径。
2. `log-dir` 日志的输出目录。
3. `log-file-name` 日志输出的文件名。
4. `--stdout` 不传该参数时，日志输出到文件；传递该参数时，日志输出到标准输出。

输出到日志文件：
```
$ consensus run -c example/config.toml -d . -f consensus.log
$ cat consensus.log
Mar 14 08:32:55.131 INFO controller grpc addr: http://127.0.0.1:50004, tag: controller, module: consensus::client:45
Mar 14 08:32:55.131 INFO network grpc addr: http://127.0.0.1:50000, tag: network, module: consensus::client:167
Mar 14 08:32:55.131 INFO registering network msg handler..., tag: network, module: consensus::client:191

```

输出到标准输出：
```
$ consensus run -c example/config.toml --stdout
Mar 14 08:34:00.124 INFO controller grpc addr: http://127.0.0.1:50004, tag: controller, module: consensus::client:45
Mar 14 08:34:00.125 INFO network grpc addr: http://127.0.0.1:50000, tag: network, module: consensus::client:167
Mar 14 08:34:00.125 INFO registering network msg handler..., tag: network, module: consensus::client:191
```


## 设计

Please check the [`ConsensusService`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/consensus.proto#L12)
and [`Consensus2ControllerService`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/controller.proto#L53)
in [cita_cloud_proto](https://github.com/cita-cloud/cita_cloud_proto)
which defines the service that consensus should implement.

The main workflow for consensus service is as follow:
1. Get proposal either from the local controller or from other remote consensus peers.
2. If the proposal comes from peers, ask the local controller to check it first.
3. Achieve consensus over the given proposal.
4. Commit the proposal with its proof to the local controller.

The proof, for example, is the nonce for POW consensus, and is empty for non-byzantine consensus like this raft implementation.
It will be used later by peers' controller to validate the corresponding block when they sync the missing blocks from others.

To communicate with other peers, you need to:
1. Implement the [`NetworkMsgHandlerService`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L39)
which handles the messages from peers.
2. Register your service to the network by [`RegisterNetworkMsgHandler`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L35),
which tells the network to forward the messages you are concerned about.

After all of that, you can send your messages to others by [`SendMsg`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L26) 
or [`Broadcast`](https://github.com/cita-cloud/cita_cloud_proto/blob/master/protos/network.proto#L29) provided by the network service.


## 实现

[raft-rs](https://github.com/tikv/raft-rs) 提供了最核心的 `Consensus Module`，而其他的组件，包括 `Log`，`State Machine`，`Transport`，都是需要应用去定制实现。

- **Storage**  
  基于`trait Storage`实现`RaftStorage`
    - **RaftStorage**

    ``` 
    impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.initial_state())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term(idx)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        self.snapshot(request_index)
    }
}
    ```
    
- **Log and State Machine**  

    `raft`的运行原理如下图所示：
    
    ![raft](img/raft.png)

    `Raft`的模型是一个基于`Log`复制的状态机模型。客户端向服务端`Leader`发起写入数据操作，`Leader`将该操作添加到`Log`并复制给所有`Follower`，当超过半数节点确认就可以将这条操作应用到`State Machine`中。
    
    通过`Log`复制的方式保证所有节点`Log`顺序一致，其目的是**保证`State Machine`中数据状态的一致性**。随着数据量的积累`Log`会不断增大，实际应用中会在适当时机对日志进行压缩，对当前`State Machine`的数据状态进行快照，将其作为应用数据的基础，并重新记录日志。一般的`Raft`应用中`Log`的数据轻，而`State Machine`的数据重，做快照的开销大，不宜频繁使用。
    而本实现作为区块链系统中的共识模块，关注重点在于利用`Raft`的`Consensus Module`。`State Machine`的数据是`ConsensusConfig`，并非真正的区块链的状态，它是为`Consensus Module`的正常运行服务的，而`Log`的数据是`Proposal`，相比之下`Log`的数据过于沉重。充分利用这一实际应用特点和日志压缩的原理，这里的做法是：每个`Proposal`被应用之后都对`State Machine`的数据状态进行快照并本地保存，并不断清空已被应用`Proposal`，数据状态一致性（`Log`查询不到会用快照同步）和重启状态恢复（本地保存的快照）都通过快照来实现。

- **Transport**  

    该能力由[network](https://cita-cloud-docs.readthedocs.io/zh_CN/latest/architecture.html#network) 实现


- **启动及运行流程**  
    ![setup](img/raft_setup.png)
  
    运行流程中的`handle ready`步骤按照[raft-rs文档](https://docs.rs/raft/latest/raft/#processing-the-ready-state) 实现

