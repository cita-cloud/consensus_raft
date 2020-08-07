#[derive(Debug)]
pub enum Error {
    RaftGroupUninitialized,
    RaftInternal(raft::Error),
    GrpcStatus(tonic::Status),
    GrpcTransport(tonic::transport::Error),
    ProtoBuf(protobuf::error::ProtobufError),
}

impl From<raft::Error> for Error {
    fn from(error: raft::Error) -> Self {
        Self::RaftInternal(error)
    }
}

impl From<tonic::Status> for Error {
    fn from(error: tonic::Status) -> Self {
        Self::GrpcStatus(error)
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(error: tonic::transport::Error) -> Self {
        Self::GrpcTransport(error)
    }
}

impl From<protobuf::error::ProtobufError> for Error {
    fn from(error: protobuf::error::ProtobufError) -> Self {
        Self::ProtoBuf(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
