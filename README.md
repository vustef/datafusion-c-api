# DataFusion C API

This project provides a C API for [Apache Arrow DataFusion](https://datafusion.apache.org/), enabling easy integration with other programming languages like Julia, Python, and more.

## Features

- Create and manage DataFusion execution contexts
- Register CSV files as tables
- Execute SQL queries
- Access query results
- Memory-safe C API with proper resource management

## Building

### Prerequisites

- Rust (1.70 or later)
- Cargo

### Build the Library

```bash
# Build in release mode (recommended)
cargo build --release

# Build in debug mode
cargo build
```

The built library will be available in:
- Release: `target/release/libdatafusion_c_api.dylib` (macOS) / `libdatafusion_c_api.so` (Linux) / `datafusion_c_api.dll` (Windows)
- Debug: `target/debug/libdatafusion_c_api.dylib` (macOS) / `libdatafusion_c_api.so` (Linux) / `datafusion_c_api.dll` (Windows)

### Generate C Headers

The C header file is automatically generated during the build process and will be available at `include/datafusion.h`.

## API Reference

### Types

- `DataFusionContext*`: Opaque pointer to a DataFusion execution context
- `DataFusionResult*`: Opaque pointer to query results

### Constants

- `DATAFUSION_OK` (0): Success
- `DATAFUSION_ERROR` (-1): Error occurred

### Functions

#### Context Management

```c
// Create a new DataFusion context
DataFusionContext* datafusion_context_new();

// Free a DataFusion context (must be called to avoid memory leaks)
void datafusion_context_free(DataFusionContext* ctx);
```

#### Data Registration

```c
// Register a CSV file as a table
int datafusion_register_csv(DataFusionContext* ctx, const char* table_name, const char* file_path);
```

#### Query Execution

```c
// Execute a SQL query
DataFusionResult* datafusion_sql(DataFusionContext* ctx, const char* sql);
```

#### Result Inspection

```c
// Get the number of record batches in a result
int datafusion_result_batch_count(const DataFusionResult* result);

// Get the number of rows in a specific batch
int datafusion_result_batch_num_rows(const DataFusionResult* result, int batch_index);

// Get the number of columns in a specific batch
int datafusion_result_batch_num_columns(const DataFusionResult* result, int batch_index);

// Print result as a formatted table (for debugging)
int datafusion_result_print(const DataFusionResult* result);

// Free a result (must be called to avoid memory leaks)
void datafusion_result_free(DataFusionResult* result);
```

#### Error Handling

```c
// Get the last error message
const char* datafusion_get_last_error();
```

## Example Usage (C)

```c
#include "include/datafusion.h"
#include <stdio.h>

int main() {
    // Create a new DataFusion context
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        printf("Failed to create DataFusion context\n");
        return 1;
    }

    // Register a CSV file
    if (datafusion_register_csv(ctx, "my_table", "data.csv") != DATAFUSION_OK) {
        printf("Failed to register CSV: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        return 1;
    }

    // Execute a SQL query
    DataFusionResult* result = datafusion_sql(ctx, "SELECT * FROM my_table LIMIT 10");
    if (!result) {
        printf("Failed to execute query: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        return 1;
    }

    // Print the results
    datafusion_result_print(result);

    // Get result statistics
    int batch_count = datafusion_result_batch_count(result);
    printf("Result contains %d batches\n", batch_count);

    if (batch_count > 0) {
        int rows = datafusion_result_batch_num_rows(result, 0);
        int cols = datafusion_result_batch_num_columns(result, 0);
        printf("First batch: %d rows, %d columns\n", rows, cols);
    }

    // Clean up
    datafusion_result_free(result);
    datafusion_context_free(ctx);

    return 0;
}
```

## Testing

### Automated C Test Suite

This project includes a comprehensive C test suite that validates all API functionality. The test suite includes:

- Context creation and destruction
- CSV file registration
- Basic SQL query execution
- Filtered queries
- Aggregation queries
- Error handling
- Result printing functionality

#### Prerequisites

Before running the tests, ensure you have:
- GCC or Clang compiler installed
- The DataFusion C API library built (see Building section above)

#### Quick Test Run (Shell Script)

The easiest way to run the tests is using the provided shell script:

```bash
# Navigate to test directory and run
cd test
./run_tests.sh
```

This script will:
- Automatically build the library if needed
- Detect available compilers (GCC and/or Clang)
- Compile and run tests with all available compilers
- Provide a summary of results

#### Running Tests with Make (Alternative)

```bash
# Build the library first
cargo build --release

# Run tests with both GCC and Clang
cd test
make all

# Run tests with GCC only
make test-gcc

# Run tests with Clang only
make test-clang

# Clean up test binaries
make clean

# See all available options
make help
```

#### Manual Compilation and Testing

If you prefer to compile manually or don't have make installed:

**With GCC:**
```bash
# Build the library
cargo build --release

# Compile the test
gcc -Wall -Wextra -std=c99 -I./include -L./target/release \
    -o test_datafusion test/test_datafusion.c \
    -ldatafusion_c_api -ldl -lpthread -lm

# Run the test (macOS/Linux)
LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH ./test_datafusion

# Run the test (macOS alternative)
DYLD_LIBRARY_PATH=./target/release:$DYLD_LIBRARY_PATH ./test_datafusion
```

**With Clang:**
```bash
# Build the library
cargo build --release

# Compile the test
clang -Wall -Wextra -std=c99 -I./include -L./target/release \
      -o test_datafusion test/test_datafusion.c \
      -ldatafusion_c_api -ldl -lpthread -lm

# Run the test (macOS/Linux)
LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH ./test_datafusion

# Run the test (macOS alternative)
DYLD_LIBRARY_PATH=./target/release:$DYLD_LIBRARY_PATH ./test_datafusion
```

#### Expected Test Output

When the tests run successfully, you should see output like:

```
DataFusion C API Test Suite
===========================

Test 1: Context creation and destruction
PASSED: Context created and freed successfully

Test 2: CSV registration
PASSED: CSV registration successful

Test 3: Basic SQL query execution
PASSED: Basic query execution successful

Test 4: Filtered query
PASSED: Filtered query successful

Test 5: Aggregation query
PASSED: Aggregation query successful

Test 6: Error handling
PASSED: Error handling works correctly

Test 7: Print result functionality
Printing query result:
+-------+-----+
| name  | age |
+-------+-----+
| Alice | 25  |
| Bob   | 30  |
| David | 28  |
| Eve   | 32  |
| Carol | 35  |
+-------+-----+
PASSED: Print result functionality works

===========================
All tests PASSED! ✓
```

#### Troubleshooting

**Library not found errors:**
- Ensure you've built the library with `cargo build --release`
- Check that the library path in compilation commands matches your build output
- On macOS, you may need to use `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH`

**Compilation errors:**
- Verify that the header file was generated at `include/datafusion.h`
- Check that you have the required system libraries (`-ldl -lpthread -lm`)
- Ensure your compiler supports C99 standard

### Simple Testing Example

For a quick test without the full suite, create a simple CSV file:

```bash
echo "id,name,age
1,Alice,25
2,Bob,30
3,Carol,35" > test.csv
```

Then use the example code from the "Example Usage (C)" section above.

## Integration

This library is designed to be used with language bindings. The primary use case is the Julia DataFusion.jl package, but it can be used with any language that supports C FFI.

## License

This project is licensed under the same terms as Apache Arrow DataFusion. 