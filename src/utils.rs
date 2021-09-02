// This is a very hacky way to map address to peer_id.
// Because raft needs an id of integer type, but only
// addresses are provided.
// This also make peer_id unreadable.
//
// According to DefaultHasher's doc, it's fine when all peers are using the same build.
// "This hasher is not guaranteed to be the same as all other DefaultHasher instances,
// but is the same as all other DefaultHasher instances created through new or default."
//
// I don't like this, maybe fix it in the future, but leaves it here for now.
pub fn address_to_peer_id(addr: &[u8]) -> u64 {
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
        format!("{}..{}", head, tail)
    }
}

#[cfg(test)]
mod tests {
    use super::address_to_peer_id;

    #[test]
    fn test_address_to_peer_id() {
        let addrs: Vec<&[u8]> = vec![b"", b"1", b"1234"];
        for addr in addrs {
            let first = address_to_peer_id(&addr);
            let second = address_to_peer_id(&addr);
            assert_eq!(first, second);
        }
    }
}
