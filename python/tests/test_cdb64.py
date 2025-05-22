import pytest
import os
from cdb64_python import CdbWriter, Cdb

@pytest.fixture
def cdb_file():
    file_path = "test.cdb"
    writer = CdbWriter(file_path)
    writer.put(b"key1", b"value1")
    writer.put(b"key2", b"value2")
    writer.put(b"anotherkey", b"anothervalue")
    writer.finalize()
    yield file_path
    os.remove(file_path)

def test_cdb_writer_and_reader(cdb_file):
    cdb = Cdb.open(cdb_file)
    assert cdb.get(b"key1") == b"value1"
    assert cdb.get(b"key2") == b"value2"
    assert cdb.get(b"anotherkey") == b"anothervalue"
    assert cdb.get(b"nonexistentkey") is None

def test_cdb_iter(cdb_file):
    cdb = Cdb.open(cdb_file)
    items = list(cdb.iter())
    # The order of items in cdb is not guaranteed, so we sort them for comparison
    # Also, convert to a set of tuples for easier comparison
    expected_items = set([(b"key1", b"value1"), (b"key2", b"value2"), (b"anotherkey", b"anothervalue")])
    
    # Convert the list of lists/tuples from cdb.iter() to a set of tuples
    actual_items = set()
    for item in items:
        # Assuming item is a list or tuple [key, value]
        actual_items.add((item[0], item[1]))

    assert actual_items == expected_items

def test_non_existent_file():
    with pytest.raises(IOError):
        Cdb.open("non_existent_file.cdb")

def test_put_after_finalize():
    file_path = "test_finalize.cdb"
    writer = CdbWriter(file_path)
    writer.put(b"key", b"value")
    writer.finalize()
    with pytest.raises(IOError):
        writer.put(b"another_key", b"another_value")
    if os.path.exists(file_path):
        os.remove(file_path)

def test_get_from_empty_cdb():
    file_path = "empty.cdb"
    writer = CdbWriter(file_path)
    writer.finalize()
    
    cdb = Cdb.open(file_path)
    assert cdb.get(b"anykey") is None
    os.remove(file_path)

def test_iter_empty_cdb():
    file_path = "empty_iter.cdb"
    writer = CdbWriter(file_path)
    writer.finalize()
    
    cdb = Cdb.open(file_path)
    items = list(cdb.iter())
    assert items == []
    os.remove(file_path)
