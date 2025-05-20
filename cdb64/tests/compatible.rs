use cdb64::{Cdb, CdbHash};
use std::fs::File;

#[test]
fn test_compatibility_with_go_cdb64() {
    let file = File::open("./tests/go_cdb64.cdb").expect("go_cdb64.cdb file should exist");
    let cdb = Cdb::<_, CdbHash>::new(file).expect("should open go_cdb64.cdb");

    // Go expectedRecords: [ ["foo", "bar"], ["baz", "quuuux"], ...]
    let expected: Vec<(&[u8], &[u8])> = vec![
        (b"foo".as_ref(), b"bar".as_ref()),
        (b"baz".as_ref(), b"quuuux".as_ref()),
        (b"playwright".as_ref(), b"wow".as_ref()),
        (b"crystal".as_ref(), b"CASTLES".as_ref()),
        (b"CRYSTAL".as_ref(), b"castles".as_ref()),
        (b"snush".as_ref(), b"collision!".as_ref()),
        (b"a".as_ref(), b"a".as_ref()),
        (b"empty_value".as_ref(), b"".as_ref()),
        (b"".as_ref(), b"empty_key".as_ref()),
    ];
    for (k, v) in expected {
        let got = cdb.get(k).expect("read should succeed");
        assert_eq!(got.as_deref(), Some(v), "key={:?}", k);
    }
    // not in the table
    let not_found = cdb.get(b"not in the table").expect("read should succeed");
    assert!(not_found.is_none());
}
