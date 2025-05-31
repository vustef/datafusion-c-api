# DataFusion C API Tests

This directory contains the C test suite for the DataFusion C API.

## Files

- `test_datafusion.c` - Main test suite with comprehensive API testing
- `Makefile` - Build and run tests with make
- `run_tests.sh` - Shell script for automated testing with multiple compilers
- `README.md` - This file

## Test Coverage

The test suite covers:

1. **Context Management**
   - Context creation and destruction
   - Memory leak prevention

2. **Data Registration**
   - CSV file registration
   - Error handling for invalid files

3. **Query Execution**
   - Basic SELECT queries
   - Filtered queries with WHERE clauses
   - Aggregation queries (COUNT, etc.)
   - Sorted queries with ORDER BY

4. **Result Inspection**
   - Batch counting
   - Row and column counting
   - Result printing functionality

5. **Error Handling**
   - Invalid file registration
   - Invalid SQL queries
   - Null pointer handling

## Running Tests

### Quick Run (Recommended)
```bash
./run_tests.sh
```

### With Make
```bash
# All compilers
make all

# GCC only
make test-gcc

# Clang only  
make test-clang

# Clean up
make clean
```

### Manual Compilation
```bash
# With GCC
gcc -Wall -Wextra -std=c99 -I../include -L../target/release \
    -o test_datafusion test_datafusion.c \
    -ldatafusion_c_api -ldl -lpthread -lm

# With Clang
clang -Wall -Wextra -std=c99 -I../include -L../target/release \
      -o test_datafusion test_datafusion.c \
      -ldatafusion_c_api -ldl -lpthread -lm
```

## Test Data

The tests use synthetic CSV data that is created programmatically:

```csv
id,name,age,department,salary
1,Alice,25,Engineering,75000
2,Bob,30,Marketing,65000
3,Carol,35,Engineering,85000
4,David,28,Sales,55000
5,Eve,32,Engineering,80000
```

This data is used to test various SQL operations and edge cases.

## Expected Behavior

All tests should pass when run against a properly built DataFusion C API library. Each test includes:

- Setup (creating test data and context)
- Execution (performing the test operation)
- Validation (checking results)
- Cleanup (freeing resources and temporary files)

## Adding New Tests

To add new tests:

1. Add a new test function following the pattern `int test_new_feature()`
2. Include the test in the `main()` function
3. Ensure proper cleanup of resources
4. Follow the existing error handling patterns 