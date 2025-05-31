# Makefile for DataFusion C API Tests

# Compiler options
CC_GCC = gcc
CC_CLANG = clang
CFLAGS = -Wall -Wextra -std=c99 -I../include -L../target/release

# Library flags
LDFLAGS = -ldatafusion_c_api -ldl -lpthread -lm

# Source files
TEST_SRC = test_datafusion.c
TEST_BIN_GCC = test_datafusion_gcc
TEST_BIN_CLANG = test_datafusion_clang

# Default target
all: test-gcc test-clang

# Build with GCC
build-gcc:
	@echo "Building test with GCC..."
	$(CC_GCC) $(CFLAGS) -o $(TEST_BIN_GCC) $(TEST_SRC) $(LDFLAGS)

# Build with Clang
build-clang:
	@echo "Building test with Clang..."
	$(CC_CLANG) $(CFLAGS) -o $(TEST_BIN_CLANG) $(TEST_SRC) $(LDFLAGS)

# Run test with GCC
test-gcc: build-gcc
	@echo "Running test with GCC binary..."
	LD_LIBRARY_PATH=../target/release:$$LD_LIBRARY_PATH ./$(TEST_BIN_GCC)

# Run test with Clang
test-clang: build-clang
	@echo "Running test with Clang binary..."
	LD_LIBRARY_PATH=../target/release:$$LD_LIBRARY_PATH ./$(TEST_BIN_CLANG)

# Clean up
clean:
	rm -f $(TEST_BIN_GCC) $(TEST_BIN_CLANG) test_employees.csv

# Help
help:
	@echo "Available targets:"
	@echo "  all         - Build and run tests with both GCC and Clang"
	@echo "  build-gcc   - Build test binary with GCC"
	@echo "  build-clang - Build test binary with Clang"
	@echo "  test-gcc    - Build and run test with GCC"
	@echo "  test-clang  - Build and run test with Clang"
	@echo "  clean       - Remove test binaries and test files"
	@echo "  help        - Show this help message"

.PHONY: all build-gcc build-clang test-gcc test-clang clean help 