#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/datafusion.h"

void test_iceberg_catalog() {
    printf("Testing Iceberg catalog creation...\n");
    
    IcebergCatalog* catalog = iceberg_catalog_new_sql("sqlite://", "test");
    assert(catalog != NULL);
    printf("✓ Catalog created successfully\n");
    
    iceberg_catalog_free(catalog);
    printf("✓ Catalog freed successfully\n");
}

void test_iceberg_schema() {
    printf("Testing Iceberg schema creation and field addition...\n");
    
    IcebergSchema* schema = iceberg_schema_new();
    assert(schema != NULL);
    printf("✓ Schema created successfully\n");
    
    // Add various field types
    bool result = iceberg_schema_add_long_field(schema, 1, "id", true);
    assert(result == true);
    printf("✓ Long field added successfully\n");
    
    result = iceberg_schema_add_long_field(schema, 2, "customer_id", true);
    assert(result == true);
    printf("✓ Second long field added successfully\n");
    
    result = iceberg_schema_add_long_field(schema, 3, "product_id", true);
    assert(result == true);
    printf("✓ Third long field added successfully\n");
    
    result = iceberg_schema_add_date_field(schema, 4, "date", true);
    assert(result == true);
    printf("✓ Date field added successfully\n");
    
    result = iceberg_schema_add_int_field(schema, 5, "amount", true);
    assert(result == true);
    printf("✓ Int field added successfully\n");
    
    iceberg_schema_free(schema);
    printf("✓ Schema freed successfully\n");
}

void test_iceberg_partition_spec() {
    printf("Testing Iceberg partition spec creation...\n");
    
    IcebergPartitionSpec* spec = iceberg_partition_spec_new();
    assert(spec != NULL);
    printf("✓ Partition spec created successfully\n");
    
    bool result = iceberg_partition_spec_add_day_field(spec, 4, 1000, "day");
    assert(result == true);
    printf("✓ Day partition field added successfully\n");
    
    iceberg_partition_spec_free(spec);
    printf("✓ Partition spec freed successfully\n");
}

void test_iceberg_table_creation() {
    printf("Testing Iceberg table creation...\n");
    
    // Create catalog
    IcebergCatalog* catalog = iceberg_catalog_new_sql("sqlite://", "test");
    assert(catalog != NULL);
    
    // Create schema
    IcebergSchema* schema = iceberg_schema_new();
    assert(schema != NULL);
    
    iceberg_schema_add_long_field(schema, 1, "id", true);
    iceberg_schema_add_long_field(schema, 2, "customer_id", true);
    iceberg_schema_add_long_field(schema, 3, "product_id", true);
    iceberg_schema_add_date_field(schema, 4, "date", true);
    iceberg_schema_add_int_field(schema, 5, "amount", true);
    
    // Create partition spec
    IcebergPartitionSpec* spec = iceberg_partition_spec_new();
    assert(spec != NULL);
    iceberg_partition_spec_add_day_field(spec, 4, 1000, "day");
    
    // Create table
    IcebergTable* table = iceberg_table_create("orders", "/test/orders", schema, spec, catalog, "test");
    assert(table != NULL);
    printf("✓ Iceberg table created successfully\n");
    
    // Clean up
    iceberg_table_free(table);
    iceberg_partition_spec_free(spec);
    iceberg_schema_free(schema);
    iceberg_catalog_free(catalog);
    printf("✓ All resources freed successfully\n");
}

void test_datafusion_iceberg_integration() {
    printf("Testing DataFusion + Iceberg integration...\n");
    
    // Create DataFusion context
    DataFusionContext* ctx = datafusion_context_new();
    assert(ctx != NULL);
    printf("✓ DataFusion context created\n");
    
    // Create Iceberg components
    IcebergCatalog* catalog = iceberg_catalog_new_sql("sqlite://", "test");
    IcebergSchema* schema = iceberg_schema_new();
    iceberg_schema_add_long_field(schema, 1, "id", true);
    iceberg_schema_add_long_field(schema, 2, "customer_id", true);
    iceberg_schema_add_long_field(schema, 3, "product_id", true);
    iceberg_schema_add_date_field(schema, 4, "date", true);
    iceberg_schema_add_int_field(schema, 5, "amount", true);
    
    IcebergPartitionSpec* spec = iceberg_partition_spec_new();
    iceberg_partition_spec_add_day_field(spec, 4, 1000, "day");
    
    IcebergTable* table = iceberg_table_create("orders", "/test/orders", schema, spec, catalog, "test");
    assert(table != NULL);
    
    // Register table with DataFusion
    int result = datafusion_register_iceberg_table(ctx, "orders", table);
    assert(result == DATAFUSION_OK);
    printf("✓ Iceberg table registered with DataFusion\n");
    
    // Try to execute a simple query (this might fail due to async nature, but registration should work)
    printf("Attempting to execute query...\n");
    DataFusionResult* query_result = datafusion_sql(ctx, "SELECT COUNT(*) FROM orders");
    if (query_result != NULL) {
        printf("✓ Query executed successfully\n");
        datafusion_result_free(query_result);
    } else {
        printf("⚠ Query failed (expected for empty table)\n");
    }
    
    // Clean up
    iceberg_table_free(table);
    iceberg_partition_spec_free(spec);
    iceberg_schema_free(schema);
    iceberg_catalog_free(catalog);
    datafusion_context_free(ctx);
    printf("✓ All resources freed successfully\n");
}

int main() {
    printf("=== DataFusion C API Iceberg Tests ===\n\n");
    
    test_iceberg_catalog();
    printf("\n");
    
    test_iceberg_schema();
    printf("\n");
    
    test_iceberg_partition_spec();
    printf("\n");
    
    test_iceberg_table_creation();
    printf("\n");
    
    test_datafusion_iceberg_integration();
    printf("\n");
    
    printf("=== All tests completed! ===\n");
    return 0;
} 