#!/bin/bash

# DataFusion C API Test Runner
# This script builds and runs the C test suite with both GCC and Clang

set -e  # Exit on any error

echo "DataFusion C API Test Runner"
echo "============================"
echo

# Check if we're in the right directory
if [ ! -f "../Cargo.toml" ]; then
    echo "ERROR: Please run this script from the test/ directory"
    exit 1
fi

# Check if library is built
if [ ! -d "../target/release" ]; then
    echo "Building DataFusion C API library..."
    cd ..
    cargo build --release
    cd test
    echo "✓ Library built successfully"
    echo
fi

# Check for required compilers
GCC_AVAILABLE=false
CLANG_AVAILABLE=false

if command -v gcc >/dev/null 2>&1; then
    GCC_AVAILABLE=true
    echo "✓ GCC found: $(gcc --version | head -n1)"
fi

if command -v clang >/dev/null 2>&1; then
    CLANG_AVAILABLE=true
    echo "✓ Clang found: $(clang --version | head -n1)"
fi

if [ "$GCC_AVAILABLE" = false ] && [ "$CLANG_AVAILABLE" = false ]; then
    echo "ERROR: Neither GCC nor Clang found. Please install a C compiler."
    exit 1
fi

echo

# Determine library path variable based on platform
if [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_PATH_VAR="DYLD_LIBRARY_PATH"
else
    LIB_PATH_VAR="LD_LIBRARY_PATH"
fi

TESTS_PASSED=0
TESTS_FAILED=0

# Test with GCC if available
if [ "$GCC_AVAILABLE" = true ]; then
    echo "Testing with GCC..."
    echo "=================="
    
    # Compile
    gcc -Wall -Wextra -std=c99 -I../include -L../target/release \
        -o test_datafusion_gcc test_datafusion.c \
        -ldatafusion_c_api -ldl -lpthread -lm
    
    # Run
    if eval "$LIB_PATH_VAR=../target/release:\$$LIB_PATH_VAR ./test_datafusion_gcc"; then
        echo "✓ GCC tests PASSED"
        ((TESTS_PASSED++))
    else
        echo "✗ GCC tests FAILED"
        ((TESTS_FAILED++))
    fi
    echo
fi

# Test with Clang if available
if [ "$CLANG_AVAILABLE" = true ]; then
    echo "Testing with Clang..."
    echo "===================="
    
    # Compile
    clang -Wall -Wextra -std=c99 -I../include -L../target/release \
          -o test_datafusion_clang test_datafusion.c \
          -ldatafusion_c_api -ldl -lpthread -lm
    
    # Run
    if eval "$LIB_PATH_VAR=../target/release:\$$LIB_PATH_VAR ./test_datafusion_clang"; then
        echo "✓ Clang tests PASSED"
        ((TESTS_PASSED++))
    else
        echo "✗ Clang tests FAILED"
        ((TESTS_FAILED++))
    fi
    echo
fi

# Summary
echo "Test Summary"
echo "============"
echo "Passed: $TESTS_PASSED"
echo "Failed: $TESTS_FAILED"

if [ $TESTS_FAILED -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "❌ Some tests failed"
    exit 1
fi 