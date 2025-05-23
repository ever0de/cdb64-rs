TARGET_DIR := ../target
RUST_TARGET_TRIPLE := $(shell rustc -vV | sed -n 's/host: //p') # Or set your specific target
LIB_NAME := libcdb64_c
HEADER_NAME := cdb64.h
C_TEST_RUNNER_NAME := c_test_runner # Changed from C_EXAMPLE_NAME
C_TEST_SRC := tests/test_main.c # Changed from C_EXAMPLE_SRC
C_INCLUDE_DIR := include

# Adjust for OS
ifeq ($(shell uname -s),Darwin)
    LIB_SUFFIX := dylib
else
    LIB_SUFFIX := so
endif

RUST_LIB_PATH := $(TARGET_DIR)/debug/$(LIB_NAME).$(LIB_SUFFIX) # Assuming debug build for now
HEADER_PATH := $(C_INCLUDE_DIR)/$(HEADER_NAME)

.PHONY: all clean build_rust build_test test

all: build_test # Changed from run_example to build_test

build_rust:
	@echo "Building Rust library (this should also generate the C header via build.rs)..."
	cargo build --manifest-path Cargo.toml

build_test: build_rust # Renamed from build_example, depends only on build_rust now
	@echo "Building C test runner..."
	$(CC) $(C_TEST_SRC) -I$(C_INCLUDE_DIR) -L$(TARGET_DIR)/debug -lcdb64_c -o $(C_TEST_RUNNER_NAME)
	@echo "C test runner built as $(C_TEST_RUNNER_NAME)"

test: build_test # Renamed from run_example
	@echo "Running C tests..."
	LD_LIBRARY_PATH=$(TARGET_DIR)/debug DYLD_LIBRARY_PATH=$(TARGET_DIR)/debug ./$(C_TEST_RUNNER_NAME)
	@echo "C tests finished."

clean:
	@echo "Cleaning up..."
	cargo clean --manifest-path Cargo.toml
	rm -f $(HEADER_PATH)
	rm -f $(C_TEST_RUNNER_NAME) # Changed from C_EXAMPLE_NAME
	rm -rf $(C_INCLUDE_DIR)
	rm -f test_c_db.cdb # Clean up test database file
