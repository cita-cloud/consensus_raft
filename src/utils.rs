use backtrace::Backtrace;
use slog::error;
use slog::Logger;
use std::panic::{self, PanicInfo};
use std::thread;

// This is a very hacky way to map node address to peer_id.
// Because raft needs an id of integer type, but only
// addresses are provided.
// This also make peer_id unreadable.
//
// According to DefaultHasher's doc, it's fine when all peers are using the same build.
// "This hasher is not guaranteed to be the same as all other DefaultHasher instances,
// but is the same as all other DefaultHasher instances created through new or default."
// "The internal algorithm is not specified, and so it and its hashes should not be relied upon over releases"
//
// I don't like this, maybe fix it in the future, but leaves it here for now.
pub fn addr_to_peer_id(addr: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(addr);
    hasher.finish()
}

// Encode binary data in hex. If it's too long, only first and last four bytes are showed.
pub fn short_hex(data: &[u8]) -> String {
    if data.len() <= 12 {
        hex::encode(data)
    } else {
        let head = hex::encode(&data[..4]);
        let tail = hex::encode(&data[data.len() - 4..]);
        format!("{head}..{tail}")
    }
}

/// Set the panic hook
pub fn set_panic_handler(logger: Logger) {
    panic::set_hook(Box::new(move |info| panic_hook(info, &logger)));
}

fn panic_hook(info: &PanicInfo, logger: &Logger) {
    let location = info.location();
    let file = location.as_ref().map(|l| l.file()).unwrap_or("<unknown>");
    let line = location.as_ref().map(|l| l.line()).unwrap_or(0);
    let msg = match info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };
    let thread = thread::current();
    let name = thread.name().unwrap_or("<unnamed>");
    let backtrace = Backtrace::new();
    let error = format!(
        "\n============================\n\
         {backtrace:?}\n\n\
         position:\n\
         Thread {name} panicked at {msg}, {file}:{line}\n\
         ============================\n\
         "
    );
    error!(logger, "{}", error);
}

pub fn clap_about() -> String {
    let name = env!("CARGO_PKG_NAME").to_string();
    let version = env!("CARGO_PKG_VERSION");
    let authors = env!("CARGO_PKG_AUTHORS");
    name + " " + version + "\n" + authors
}

#[cfg(test)]
mod tests {
    use super::addr_to_peer_id;

    #[test]
    fn test_address_to_peer_id() {
        let addrs: Vec<&[u8]> = vec![b"", b"1", b"1234"];
        for addr in addrs {
            // check coherence
            let first = addr_to_peer_id(addr);
            let second = addr_to_peer_id(addr);
            assert_eq!(first, second);
        }
    }
}
