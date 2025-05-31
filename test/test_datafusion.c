#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include "../include/datafusion.h"

// Test data
static const char* test_csv_data = 
    "id,name,age,department,salary\n"
    "1,Alice,25,Engineering,75000\n"
    "2,Bob,30,Marketing,65000\n"
    "3,Carol,35,Engineering,85000\n"
    "4,David,28,Sales,55000\n"
    "5,Eve,32,Engineering,80000\n";

static const char* test_csv_path = "test_employees.csv";

// Helper function to create test CSV file
int create_test_csv() {
    FILE* file = fopen(test_csv_path, "w");
    if (!file) {
        printf("ERROR: Failed to create test CSV file\n");
        return -1;
    }
    
    fprintf(file, "%s", test_csv_data);
    fclose(file);
    return 0;
}

// Helper function to cleanup test files
void cleanup_test_files() {
    if (access(test_csv_path, F_OK) == 0) {
        unlink(test_csv_path);
    }
}

// Test 1: Context creation and destruction
int test_context_creation() {
    printf("Test 1: Context creation and destruction\n");
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        printf("FAILED: Could not create DataFusion context\n");
        return -1;
    }
    
    datafusion_context_free(ctx);
    printf("PASSED: Context created and freed successfully\n");
    return 0;
}

// Test 2: CSV registration
int test_csv_registration() {
    printf("Test 2: CSV registration\n");
    
    if (create_test_csv() != 0) {
        return -1;
    }
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        cleanup_test_files();
        return -1;
    }
    
    int result = datafusion_register_csv(ctx, "employees", test_csv_path);
    if (result != DATAFUSION_OK) {
        printf("FAILED: Could not register CSV file: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: CSV registration successful\n");
    return 0;
}

// Test 3: Basic SQL query execution
int test_basic_query() {
    printf("Test 3: Basic SQL query execution\n");
    
    if (create_test_csv() != 0) {
        return -1;
    }
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        cleanup_test_files();
        return -1;
    }
    
    if (datafusion_register_csv(ctx, "employees", test_csv_path) != DATAFUSION_OK) {
        printf("FAILED: Could not register CSV file\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    DataFusionResult* result = datafusion_sql(ctx, "SELECT * FROM employees");
    if (!result) {
        printf("FAILED: Could not execute SQL query: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    // Check result metadata
    int batch_count = datafusion_result_batch_count(result);
    if (batch_count < 1) {
        printf("FAILED: Expected at least 1 batch, got %d\n", batch_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    int row_count = datafusion_result_batch_num_rows(result, 0);
    if (row_count != 5) {
        printf("FAILED: Expected 5 rows, got %d\n", row_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    int col_count = datafusion_result_batch_num_columns(result, 0);
    if (col_count != 5) {
        printf("FAILED: Expected 5 columns, got %d\n", col_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_result_free(result);
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: Basic query execution successful\n");
    return 0;
}

// Test 4: Filtered query
int test_filtered_query() {
    printf("Test 4: Filtered query\n");
    
    if (create_test_csv() != 0) {
        return -1;
    }
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        cleanup_test_files();
        return -1;
    }
    
    if (datafusion_register_csv(ctx, "employees", test_csv_path) != DATAFUSION_OK) {
        printf("FAILED: Could not register CSV file\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    DataFusionResult* result = datafusion_sql(ctx, "SELECT name FROM employees WHERE age > 30");
    if (!result) {
        printf("FAILED: Could not execute filtered query: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    int row_count = datafusion_result_batch_num_rows(result, 0);
    if (row_count != 2) {  // Bob and Carol
        printf("FAILED: Expected 2 rows for age > 30, got %d\n", row_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_result_free(result);
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: Filtered query successful\n");
    return 0;
}

// Test 5: Aggregation query
int test_aggregation_query() {
    printf("Test 5: Aggregation query\n");
    
    if (create_test_csv() != 0) {
        return -1;
    }
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        cleanup_test_files();
        return -1;
    }
    
    if (datafusion_register_csv(ctx, "employees", test_csv_path) != DATAFUSION_OK) {
        printf("FAILED: Could not register CSV file\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    DataFusionResult* result = datafusion_sql(ctx, "SELECT COUNT(*) as total FROM employees");
    if (!result) {
        printf("FAILED: Could not execute aggregation query: %s\n", datafusion_get_last_error());
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    int row_count = datafusion_result_batch_num_rows(result, 0);
    if (row_count != 1) {
        printf("FAILED: Expected 1 row for COUNT query, got %d\n", row_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    int col_count = datafusion_result_batch_num_columns(result, 0);
    if (col_count != 1) {
        printf("FAILED: Expected 1 column for COUNT query, got %d\n", col_count);
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_result_free(result);
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: Aggregation query successful\n");
    return 0;
}

// Test 6: Error handling
int test_error_handling() {
    printf("Test 6: Error handling\n");
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        return -1;
    }
    
    // Test invalid CSV file
    int result = datafusion_register_csv(ctx, "invalid", "nonexistent.csv");
    if (result == DATAFUSION_OK) {
        printf("FAILED: Expected error for nonexistent CSV file\n");
        datafusion_context_free(ctx);
        return -1;
    }
    
    // Test invalid SQL after registering valid table
    if (create_test_csv() != 0) {
        datafusion_context_free(ctx);
        return -1;
    }
    
    if (datafusion_register_csv(ctx, "employees", test_csv_path) != DATAFUSION_OK) {
        printf("FAILED: Could not register valid CSV file\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    DataFusionResult* result_ptr = datafusion_sql(ctx, "SELECT * FROM nonexistent_table");
    if (result_ptr != NULL) {
        printf("FAILED: Expected error for invalid table reference\n");
        datafusion_result_free(result_ptr);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: Error handling works correctly\n");
    return 0;
}

// Test 7: Print result functionality
int test_print_result() {
    printf("Test 7: Print result functionality\n");
    
    if (create_test_csv() != 0) {
        return -1;
    }
    
    DataFusionContext* ctx = datafusion_context_new();
    if (!ctx) {
        cleanup_test_files();
        return -1;
    }
    
    if (datafusion_register_csv(ctx, "employees", test_csv_path) != DATAFUSION_OK) {
        printf("FAILED: Could not register CSV file\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    DataFusionResult* result = datafusion_sql(ctx, "SELECT name, age FROM employees ORDER BY age");
    if (!result) {
        printf("FAILED: Could not execute query for print test\n");
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    printf("Printing query result:\n");
    int print_result = datafusion_result_print(result);
    if (print_result != DATAFUSION_OK) {
        printf("FAILED: Could not print result\n");
        datafusion_result_free(result);
        datafusion_context_free(ctx);
        cleanup_test_files();
        return -1;
    }
    
    datafusion_result_free(result);
    datafusion_context_free(ctx);
    cleanup_test_files();
    printf("PASSED: Print result functionality works\n");
    return 0;
}

int main() {
    printf("DataFusion C API Test Suite\n");
    printf("===========================\n\n");
    
    int failed_tests = 0;
    
    if (test_context_creation() != 0) failed_tests++;
    printf("\n");
    
    if (test_csv_registration() != 0) failed_tests++;
    printf("\n");
    
    if (test_basic_query() != 0) failed_tests++;
    printf("\n");
    
    if (test_filtered_query() != 0) failed_tests++;
    printf("\n");
    
    if (test_aggregation_query() != 0) failed_tests++;
    printf("\n");
    
    if (test_error_handling() != 0) failed_tests++;
    printf("\n");
    
    if (test_print_result() != 0) failed_tests++;
    printf("\n");
    
    printf("===========================\n");
    if (failed_tests == 0) {
        printf("All tests PASSED! ✓\n");
        return 0;
    } else {
        printf("%d test(s) FAILED! ✗\n", failed_tests);
        return 1;
    }
} 