# consensus_raft Agent Profile

## 项目基本信息
- **项目名称**: consensus_raft
- **版本**: 6.7.5
- **描述**: 该项目是 CITA-Cloud 架构下的共识微服务实现，基于 TiKV 的 `raft-rs` 库构建。它负责在分布式环境中对交易提案（Proposal）达成共识，并与 Controller 和 Network 微服务进行交互。
- **源码仓库**: [consensus_raft](https://github.com/cita-cloud/consensus_raft)

## 核心能力
1.  **Raft 共识算法**: 
    - 实现标准的 Raft 共识协议，包括领导者选举（Leader Election）、日志复制（Log Replication）和安全性保障。
    - 使用 `raft-rs` 作为底层 Raft 状态机实现。
2.  **微服务交互**:
    - **Controller 交互**: 通过 gRPC 向 Controller 获取提案 (`get_proposal`)、验证提案 (`check_proposal`) 和提交已达成共识的区块 (`commit_block`)。
    - **Network 交互**: 通过 Network 微服务广播和接收 Raft 消息，实现节点间的通信。
3.  **动态配置变更**:
    - 支持运行时更新共识节点集合（Validators），处理 `ConsensusConfiguration` 变更。
4.  **持久化存储**:
    - 实现了 `RaftStorage`，将 Raft 日志 (`raft-entry`) 和快照 (`raft-snapshot`) 持久化到本地磁盘，支持节点重启后的状态恢复。
5.  **快照机制**:
    - 支持生成和应用 Raft 快照，用于压缩日志和快速同步落后节点的状态。
6.  **监控指标**:
    - 集成 Prometheus 指标（Metrics），可通过配置文件开启，用于监控共识状态。

## 运行依赖
- **操作系统**: Linux
- **微服务依赖**:
    - `controller`: 提供提案生成、验证和区块提交服务 (默认 gRPC 端口: 50004)。
    - `network`: 提供节点间网络通信服务 (默认 gRPC 端口: 50000)。
- **配置文件**:
    - `config.toml`: 服务配置文件。
    - `node_address`: 节点身份标识文件。

## 关键文件结构
- `src/lib.rs`: 模块入口。
- `src/peer.rs`: Raft 节点核心逻辑 (`Peer` 结构体)，处理 Raft 消息驱动和状态流转。
- `src/client.rs`: gRPC 客户端封装，用于连接 Controller 和 Network。
- `src/storage.rs`: Raft 日志和快照的持久化存储实现。
- `src/config.rs`: 配置加载与解析。
- `main.rs`: 程序入口，负责命令行解析和启动服务。

## 使用示例

### 1. 编译
```bash
cargo build --release
```

### 2. 配置文件示例 (config.toml)
```toml
# 依赖的 controller 微服务端口
controller_port = 50004
# 本服务 gRPC 监听端口
grpc_listen_port = 50001
# 依赖的 network 微服务端口
network_port = 50000
# 监控指标端口
metrics_port = 60001
# 是否开启监控
enable_metrics = true
# 节点地址文件路径
node_addr = 'node_address'
# 日志配置 (可选)
log_dir = "logs"
log_file_name = "consensus.log"
```

### 3. 运行服务
```bash
# 基本运行
./target/release/consensus run -c config.toml

# 指定日志目录和文件
./target/release/consensus run -c config.toml -d ./logs -f consensus.log

# 输出日志到标准输出
./target/release/consensus run -c config.toml --stdout
```

### 4. 命令行帮助
```bash
$ consensus -h
consensus 6.7.5
Rivtower Technologies <contact@rivtower.com>

Usage: consensus [COMMAND]

Commands:
  run   run the service
  help  Print this message or the help of the given subcommand(s)
```
