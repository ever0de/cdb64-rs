#include <stdio.h>
#include <string.h>
#include <stdlib.h> // For EXIT_FAILURE, EXIT_SUCCESS
#include "../include/cdb64.h" // Adjust path as necessary

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

int main() {
    const char *db_path = "test_c_db.cdb";
    cdb_CdbWriterFile *writer = NULL; // Corrected type
    cdb_CdbFile *reader = NULL;       // Corrected type
    int ret;

    printf("Creating CDB writer for: %s\n", db_path);
    writer = cdb_writer_create(db_path);
    if (!writer) {
        fprintf(stderr, "Failed to create CDB writer.\n");
        return EXIT_FAILURE;
    }

    unsigned char key1[] = "hello";
    unsigned char value1[] = "c world";
    printf("Putting key: '%s', value: '", key1);
    print_hex(value1, sizeof(value1) -1);
    ret = cdb_writer_put(writer, key1, sizeof(key1) - 1, value1, sizeof(value1) - 1);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to put key1. Error code: %d\n", ret);
        cdb_writer_free(writer);
        return EXIT_FAILURE;
    }

    unsigned char key2[] = {0x01, 0x02, 0x03};
    unsigned char value2[] = {0xAA, 0xBB, 0xCC, 0xDD};
    printf("Putting key: "); print_hex(key2, sizeof(key2));
    printf("Value: "); print_hex(value2, sizeof(value2));
    ret = cdb_writer_put(writer, key2, sizeof(key2), value2, sizeof(value2));
     if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to put key2. Error code: %d\n", ret);
        cdb_writer_free(writer);
        return EXIT_FAILURE;
    }

    printf("Finalizing writer...\n");
    ret = cdb_writer_finalize(writer);
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to finalize writer. Error code: %d\n", ret);
        cdb_writer_free(writer);
        return EXIT_FAILURE;
    }
    cdb_writer_free(writer);
    writer = NULL;
    printf("Writer finalized and freed.\n");

    // --- Reading ---
    printf("\nOpening CDB reader for: %s\n", db_path);
    reader = cdb_open(db_path);
    if (!reader) {
        fprintf(stderr, "Failed to open CDB reader.\n");
        return EXIT_FAILURE;
    }
    printf("Reader opened.\n");

    cdb_CdbData val_data; // Corrected type

    // Get key1
    printf("Getting key: '%s'\\n", (char*)key1); // Keep cast for printf if key1 is string-like
    ret = cdb_get(reader, key1, sizeof(key1) - 1, &val_data); 
    if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to get key1. Error code: %d\\n", ret);
        // Consider this a test failure
        cdb_close(reader);
        return EXIT_FAILURE;
    } else {
        if (val_data.ptr) {
            printf("Value for '%s': ", (char*)key1); 
            for (size_t i = 0; i < val_data.len; ++i) {
                printf("%c", ((char*)val_data.ptr)[i]); 
            }
            printf(" (hex: ");
            print_hex((const unsigned char*)val_data.ptr, val_data.len); 
            printf(")\\n");
            // Basic check: ensure value is what we expect (e.g., "c world")
            if (val_data.len != (sizeof(value1) -1) || memcmp(val_data.ptr, value1, val_data.len) != 0) {
                fprintf(stderr, "TEST FAILED: Value for key1 does not match expected value.\\n");
                cdb_free_data(val_data);
                cdb_close(reader);
                return EXIT_FAILURE;
            }
            cdb_free_data(val_data);
        } else {
            fprintf(stderr, "TEST FAILED: Key '%s' not found but was expected.\\n", (char*)key1); 
            cdb_close(reader);
            return EXIT_FAILURE;
        }
    }
    
    // Get key2
    printf("Getting key: "); print_hex(key2, sizeof(key2));
    ret = cdb_get(reader, key2, sizeof(key2), &val_data); 
     if (ret != CDB_SUCCESS) {
        fprintf(stderr, "Failed to get key2. Error code: %d\\n", ret);
        // Consider this a test failure
        cdb_close(reader);
        return EXIT_FAILURE;
    } else {
        if (val_data.ptr) {
            printf("Value for key (hex): "); print_hex(key2, sizeof(key2));
            printf("is (hex): "); print_hex((const unsigned char*)val_data.ptr, val_data.len); 
            // Basic check: ensure value is what we expect
             if (val_data.len != sizeof(value2) || memcmp(val_data.ptr, value2, val_data.len) != 0) {
                fprintf(stderr, "TEST FAILED: Value for key2 does not match expected value.\\n");
                cdb_free_data(val_data);
                cdb_close(reader);
                return EXIT_FAILURE;
            }
            cdb_free_data(val_data);
        } else {
            fprintf(stderr, "TEST FAILED: Key (hex) "); print_hex(key2, sizeof(key2)); fprintf(stderr, "not found but was expected.\\n");
            cdb_close(reader);
            return EXIT_FAILURE;
        }
    }

    // Get non-existent key
    unsigned char key_not_found[] = "not_found_key";
    printf("Getting key: '%s'\\n", (char*)key_not_found); 
    ret = cdb_get(reader, key_not_found, sizeof(key_not_found) - 1, &val_data); 
    if (ret != CDB_SUCCESS) {
         fprintf(stderr, "Error when getting non_existent_key. Error code: %d\\n", ret);
         // This could be an error in cdb_get itself, or an unexpected issue.
         // Depending on strictness, this could also be a test failure.
         // For now, let's assume cdb_get should return CDB_SUCCESS even if key is not found.
    } else {
        if (val_data.ptr) {
            fprintf(stderr, "TEST FAILED: Unexpectedly found value for '%s'\\n", (char*)key_not_found); 
            cdb_free_data(val_data);
            cdb_close(reader);
            return EXIT_FAILURE;
        } else {
            printf("Key '%s' correctly not found.\\n", (char*)key_not_found); 
        }
    }

    printf("Closing reader...\\n");
    cdb_close(reader);
    reader = NULL;
    printf("Reader closed.\\n");

    printf("\nC FFI example finished successfully.\n");
    return EXIT_SUCCESS;
}
