TARGET_DIR := ../target
RUST_TARGET_TRIPLE := $(strip $(shell rustc -vV | sed -n 's/host: //p')) # Or set your specific target
LIB_NAME := libcdb64_c
HEADER_NAME := cdb64.h
C_TEST_RUNNER_NAME := integration_test
C_TEST_RUNNER_ASAN_NAME := integration_test_asan
C_TEST_SRC := tests/integration_test.c
C_INCLUDE_DIR := include

# Adjust for OS
ifeq ($(shell uname -s),Darwin)
    LIB_SUFFIX := dylib
    # On macOS, LSan is bundled with ASan. MallocStackLogging is not needed.
    ASAN_ENV := MallocNanoZone=0 ASAN_OPTIONS=detect_leaks=1
else
    LIB_SUFFIX := so
    ASAN_ENV := ASAN_OPTIONS=detect_leaks=1
endif

RUST_LIB_PATH := $(TARGET_DIR)/debug/$(LIB_NAME).$(LIB_SUFFIX) # Assuming debug build for now
HEADER_PATH := $(C_INCLUDE_DIR)/$(HEADER_NAME)

.PHONY: all clean build_rust build_test test build_test_asan test-asan

all: test

build_rust:
	@echo "Building Rust library (this should also generate the C header via build.rs)..."
	cargo build --manifest-path Cargo.toml

build_test: build_rust
	@echo "Building C test runner..."
	$(CC) $(C_TEST_SRC) -I$(C_INCLUDE_DIR) -L$(TARGET_DIR)/debug -lcdb64_c -o $(C_TEST_RUNNER_NAME)
	@echo "C test runner built: $(C_TEST_RUNNER_NAME)"

test: build_test
	@echo "Running comprehensive C tests..."
	LD_LIBRARY_PATH=$(TARGET_DIR)/debug DYLD_LIBRARY_PATH=$(TARGET_DIR)/debug ./$(C_TEST_RUNNER_NAME)
	@echo "All C tests finished successfully."

# Build the Rust library instrumented with AddressSanitizer.
# Requires nightly Rust and the rust-src component:
#   rustup toolchain install nightly
#   rustup component add --toolchain nightly rust-src
build_rust_asan:
	@echo "Building Rust library with AddressSanitizer..."
	RUSTFLAGS="-Z sanitizer=address" \
	  cargo +nightly build -Z build-std \
	    --target $(RUST_TARGET_TRIPLE) \
	    --manifest-path Cargo.toml
	@echo "ASan Rust library built."

# Build the C test runner linked against the ASan-instrumented Rust library.
build_test_asan: build_rust_asan
	@echo "Building C test runner with ASan..."
	$(CC) -fsanitize=address -g \
	  $(C_TEST_SRC) \
	  -I$(C_INCLUDE_DIR) \
	  -L$(TARGET_DIR)/$(RUST_TARGET_TRIPLE)/debug \
	  -lcdb64_c \
	  -o $(C_TEST_RUNNER_ASAN_NAME)
	@echo "ASan C test runner built: $(C_TEST_RUNNER_ASAN_NAME)"

# Run the C integration tests under AddressSanitizer + LeakSanitizer.
test-asan: build_test_asan
	@echo "Running C tests with AddressSanitizer + LeakSanitizer..."
	$(ASAN_ENV) \
	  LD_LIBRARY_PATH=$(TARGET_DIR)/$(RUST_TARGET_TRIPLE)/debug \
	  DYLD_LIBRARY_PATH=$(TARGET_DIR)/$(RUST_TARGET_TRIPLE)/debug \
	  ./$(C_TEST_RUNNER_ASAN_NAME)
	@echo "ASan C tests finished."

clean:
	@echo "Cleaning up..."
	cargo clean --manifest-path Cargo.toml
	rm -f $(HEADER_PATH)
	rm -f $(C_TEST_RUNNER_NAME) $(C_TEST_RUNNER_ASAN_NAME)
	rm -rf $(C_INCLUDE_DIR)
	rm -f test_c_db.cdb test_iterator.cdb test_empty.cdb # Clean up test database files
