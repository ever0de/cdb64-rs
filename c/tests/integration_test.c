#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "../include/cdb64.h"

// Define CDB_SUCCESS if not already defined (e.g. by the header)
#ifndef CDB_SUCCESS
#define CDB_SUCCESS 0
#endif

void print_hex(const unsigned char *data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        printf("%02x", data[i]);
    }
    printf("\n");
}

void test_basic_functionality() {
    printf("=== Testing Basic CDB Functionality ===\n");
    
    const char *db_path = "test_c_db.cdb";
    cdb_CdbWriterFile *writer = NULL;
    cdb_CdbFile *reader = NULL;
    int ret;

    printf("Creating CDB writer for: %s\n", db_path);
    writer = cdb_writer_create(db_path);
    if (!writer) {
        fprintf(stderr, "Failed to create CDB writer.\n");
        exit(EXIT_FAILURE);
    }

    unsigned char key1[] = "hello";
    unsigned char value1[] = "c world";
    printf("Putting key: '%s', value: ", key1);
    print_hex(value1, sizeof(value1) -1);
    ret = cdb_writer_put(writer, key1, sizeof(key1) - 1, value1, sizeof(value1) - 1);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to put key1. Error code: %d\n", ret);
        cdb_writer_free(writer);
        exit(EXIT_FAILURE);
    }

    unsigned char key2[] = {0x01, 0x02, 0x03};
    unsigned char value2[] = {0xAA, 0xBB, 0xCC, 0xDD};
    printf("Putting key: "); print_hex(key2, sizeof(key2));
    printf("Value: "); print_hex(value2, sizeof(value2));
    ret = cdb_writer_put(writer, key2, sizeof(key2), value2, sizeof(value2));
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to put key2. Error code: %d\n", ret);
        cdb_writer_free(writer);
        exit(EXIT_FAILURE);
    }

    printf("Finalizing writer...\n");
    ret = cdb_writer_finalize(writer);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to finalize writer. Error code: %d\n", ret);
        cdb_writer_free(writer);
        exit(EXIT_FAILURE);
    }
    cdb_writer_free(writer);
    writer = NULL;
    printf("Writer finalized and freed.\n");

    // --- Reading ---
    printf("\nOpening CDB reader for: %s\n", db_path);
    reader = cdb_open(db_path);
    if (!reader) {
        fprintf(stderr, "Failed to open CDB reader.\n");
        exit(EXIT_FAILURE);
    }
    printf("Reader opened.\n");

    cdb_CdbData val_data;

    // Get key1
    printf("Getting key: '%s'\\n", (char*)key1);
    ret = cdb_get(reader, key1, sizeof(key1) - 1, &val_data); 
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to get key1. Error code: %d\\n", ret);
        cdb_close(reader);
        exit(EXIT_FAILURE);
    } else {
        if (val_data.ptr) {
            printf("Value for '%s': ", (char*)key1); 
            for (size_t i = 0; i < val_data.len; ++i) {
                printf("%c", ((char*)val_data.ptr)[i]); 
            }
            printf(" hex: ");
            print_hex((const unsigned char*)val_data.ptr, val_data.len); 
            // Basic check: ensure value is what we expect (e.g., "c world")
            if (val_data.len != (sizeof(value1) -1) || memcmp(val_data.ptr, value1, val_data.len) != 0) {
                fprintf(stderr, "TEST FAILED: Value for key1 does not match expected value.\\n");
                cdb_free_data(val_data);
                cdb_close(reader);
                exit(EXIT_FAILURE);
            }
            cdb_free_data(val_data);
        } else {
            fprintf(stderr, "TEST FAILED: Key '%s' not found but was expected.\\n", (char*)key1); 
            cdb_close(reader);
            exit(EXIT_FAILURE);
        }
    }
    
    // Get key2
    printf("Getting key: "); print_hex(key2, sizeof(key2));
    ret = cdb_get(reader, key2, sizeof(key2), &val_data); 
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to get key2. Error code: %d\\n", ret);
        cdb_close(reader);
        exit(EXIT_FAILURE);
    } else {
        if (val_data.ptr) {
            printf("Value for key (hex): "); print_hex(key2, sizeof(key2));
            printf("is (hex): "); print_hex((const unsigned char*)val_data.ptr, val_data.len); 
            // Basic check: ensure value is what we expect
            if (val_data.len != sizeof(value2) || memcmp(val_data.ptr, value2, val_data.len) != 0) {
                fprintf(stderr, "TEST FAILED: Value for key2 does not match expected value.\\n");
                cdb_free_data(val_data);
                cdb_close(reader);
                exit(EXIT_FAILURE);
            }
            cdb_free_data(val_data);
        } else {
            fprintf(stderr, "TEST FAILED: Key (hex) "); print_hex(key2, sizeof(key2)); fprintf(stderr, "not found but was expected.\\n");
            cdb_close(reader);
            exit(EXIT_FAILURE);
        }
    }

    // Get non-existent key - FIXED: Handle the error properly
    unsigned char key_not_found[] = "not_found_key";
    printf("Getting key: '%s'\\n", (char*)key_not_found); 
    ret = cdb_get(reader, key_not_found, sizeof(key_not_found) - 1, &val_data); 
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Error when getting non_existent_key. Error code: %d\\n", ret);
        // This could be an error in cdb_get itself, or an unexpected issue.
        // Depending on strictness, this could also be a test failure.
        // For now, let's assume cdb_get should return CDB_SUCCESS even if key is not found.
        cdb_close(reader);
        exit(EXIT_FAILURE);
    } else {
        if (val_data.ptr) {
            fprintf(stderr, "TEST FAILED: Unexpectedly found value for '%s'\\n", (char*)key_not_found); 
            cdb_free_data(val_data);
            cdb_close(reader);
            exit(EXIT_FAILURE);
        } else {
            printf("Key '%s' correctly not found.\\n", (char*)key_not_found); 
        }
    }

    printf("Closing reader...\\n");
    cdb_close(reader);
    reader = NULL;
    printf("Reader closed.\\n");

    // Clean up test file
    remove(db_path);
    
    printf("=== Basic Functionality Test Completed Successfully! ===\n\n");
}

void test_iterator() {
    printf("=== Testing Iterator Functionality ===\n");
    
    const char* test_file = "test_iterator.cdb";
    
    // Create a test CDB file with some data
    printf("1. Creating test CDB file...\n");
    cdb_CdbWriterFile* writer = cdb_writer_create(test_file);
    assert(writer != NULL);
    
    // Add test data
    const char* keys[] = {"key1", "key2", "key3", "key4", "key5"};
    const char* values[] = {"value1", "value2", "value3", "value4", "value5"};
    const int num_entries = 5;
    
    for (int i = 0; i < num_entries; i++) {
        int result = cdb_writer_put(writer, 
                                  (const unsigned char*)keys[i], strlen(keys[i]),
                                  (const unsigned char*)values[i], strlen(values[i]));
        assert(result == cdb_CDB_SUCCESS);
        printf("   Added: %s -> %s\n", keys[i], values[i]);
    }
    
    int finalize_result = cdb_writer_finalize(writer);
    assert(finalize_result == cdb_CDB_SUCCESS);
    cdb_writer_free(writer);
    printf("   CDB file created successfully.\n");
    
    // Test iterator functionality
    printf("2. Testing iterator...\n");
    cdb_CdbFile* reader = cdb_open(test_file);
    assert(reader != NULL);
    
    // Create iterator (this transfers ownership of reader to iterator)
    cdb_OwnedCdbIterator* iterator = cdb_iterator_new(reader);
    assert(iterator != NULL);
    printf("   Iterator created successfully.\n");
    
    // Note: reader should not be used after cdb_iterator_new()
    // reader = NULL; // Set to NULL to avoid accidental use
    
    // Iterate through all entries
    printf("3. Iterating through entries...\n");
    cdb_CdbKeyValue kv;
    int count = 0;
    int result;
    
    while ((result = cdb_iterator_next(iterator, &kv)) == cdb_CDB_ITERATOR_HAS_NEXT) {
        // Convert key and value to null-terminated strings for printing
        char* key_str = malloc(kv.key.len + 1);
        char* value_str = malloc(kv.value.len + 1);
        
        memcpy(key_str, kv.key.ptr, kv.key.len);
        key_str[kv.key.len] = '\0';
        
        memcpy(value_str, kv.value.ptr, kv.value.len);
        value_str[kv.value.len] = '\0';
        
        printf("   Entry %d: %s -> %s\n", count + 1, key_str, value_str);
        
        // Free the allocated memory for key and value
        cdb_free_data(kv.key);
        cdb_free_data(kv.value);
        
        free(key_str);
        free(value_str);
        
        count++;
    }
    
    printf("   Iteration finished. Result: %d\n", result);
    assert(result == cdb_CDB_ITERATOR_FINISHED);
    assert(count == num_entries);
    printf("   Successfully iterated through %d entries.\n", count);
    
    // Test that iterator is finished
    printf("4. Testing iterator exhaustion...\n");
    result = cdb_iterator_next(iterator, &kv);
    assert(result == cdb_CDB_ITERATOR_FINISHED);
    assert(kv.key.ptr == NULL);
    assert(kv.value.ptr == NULL);
    printf("   Iterator correctly reports exhaustion.\n");
    
    // Clean up
    cdb_iterator_free(iterator);
    printf("   Iterator freed.\n");
    
    // Remove test file
    remove(test_file);
    
    printf("=== Iterator Test Completed Successfully! ===\n\n");
}

void test_iterator_empty_database() {
    printf("=== Testing Iterator on Empty Database ===\n");
    
    const char* test_file = "test_empty.cdb";
    
    // Create empty CDB file
    printf("1. Creating empty CDB file...\n");
    cdb_CdbWriterFile* writer = cdb_writer_create(test_file);
    assert(writer != NULL);
    
    int finalize_result = cdb_writer_finalize(writer);
    assert(finalize_result == cdb_CDB_SUCCESS);
    cdb_writer_free(writer);
    printf("   Empty CDB file created.\n");
    
    // Test iterator on empty database
    printf("2. Testing iterator on empty database...\n");
    cdb_CdbFile* reader = cdb_open(test_file);
    assert(reader != NULL);
    
    cdb_OwnedCdbIterator* iterator = cdb_iterator_new(reader);
    assert(iterator != NULL);
    
    cdb_CdbKeyValue kv;
    int result = cdb_iterator_next(iterator, &kv);
    
    assert(result == cdb_CDB_ITERATOR_FINISHED);
    assert(kv.key.ptr == NULL);
    assert(kv.value.ptr == NULL);
    printf("   Iterator correctly handles empty database.\n");
    
    // Clean up
    cdb_iterator_free(iterator);
    remove(test_file);
    
    printf("=== Empty Database Iterator Test Completed! ===\n\n");
}

void test_iterator_error_handling() {
    printf("=== Testing Iterator Error Handling ===\n");
    
    // Test null pointer handling
    printf("1. Testing null pointer handling...\n");
    
    cdb_CdbKeyValue kv;
    int result = cdb_iterator_next(NULL, &kv);
    assert(result == cdb_CDB_ERROR_NULL_POINTER);
    printf("   Null iterator pointer correctly handled.\n");
    
    // This would be harder to test without creating an invalid iterator
    // For now, we'll just test the basic null case
    
    printf("=== Error Handling Test Completed! ===\n\n");
}

int main() {
    printf("Starting Comprehensive C Tests for CDB64\n");
    printf("========================================\n\n");
    
    test_basic_functionality();
    test_iterator();
    test_iterator_empty_database();
    test_iterator_error_handling();
    
    printf("All tests passed successfully!\n");
    return EXIT_SUCCESS;
}
