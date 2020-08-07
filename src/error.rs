use raft::Error as RaftError;

#[derive(Debug)]
pub enum Error {
    RaftGroupUninitialized,
    RaftInternal(RaftError),
    GrpcStatus(tonic::Status),
    GrpcTransport(tonic::transport::Error),
}

impl From<RaftError> for Error {
    fn from(error: RaftError) -> Self {
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

pub type Result<T> = std::result::Result<T, Error>;
