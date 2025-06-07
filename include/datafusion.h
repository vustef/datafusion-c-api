#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define DATAFUSION_OK 0

#define DATAFUSION_ERROR -1

typedef struct DataFusionContext DataFusionContext;

typedef struct DataFusionResult DataFusionResult;

typedef struct IcebergCatalog IcebergCatalog;

typedef struct IcebergPartitionSpec IcebergPartitionSpec;

typedef struct IcebergSchema IcebergSchema;

typedef struct IcebergTable IcebergTable;

/**
 * Create a new DataFusion context
 * Returns a pointer to the context or null on error
 */
struct DataFusionContext *datafusion_context_new(void);

/**
 * Free a DataFusion context
 */
void datafusion_context_free(struct DataFusionContext *ctx);

/**
 * Register a CSV file with the context
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
int datafusion_register_csv(struct DataFusionContext *ctx,
                            const char *table_name,
                            const char *file_path);

/**
 * Execute a SQL query
 * Returns a pointer to the result or null on error
 */
struct DataFusionResult *datafusion_sql(struct DataFusionContext *ctx, const char *sql);

/**
 * Get the number of batches in a result
 */
int datafusion_result_batch_count(const struct DataFusionResult *result);

/**
 * Get the number of rows in a specific batch
 */
int datafusion_result_batch_num_rows(const struct DataFusionResult *result, int batch_index);

/**
 * Get the number of columns in a specific batch
 */
int datafusion_result_batch_num_columns(const struct DataFusionResult *result, int batch_index);

/**
 * Print a result as a formatted table (for debugging)
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
int datafusion_result_print(const struct DataFusionResult *result);

/**
 * Free a DataFusion result
 */
void datafusion_result_free(struct DataFusionResult *result);

/**
 * Get last error message (simplified for this example)
 */
const char *datafusion_get_last_error(void);

/**
 * Create a new SQL catalog for Iceberg
 * Returns a pointer to the catalog or null on error
 */
struct IcebergCatalog *iceberg_catalog_new_sql(const char *database_url, const char *name);

/**
 * Free an Iceberg catalog
 */
void iceberg_catalog_free(struct IcebergCatalog *catalog);

/**
 * Create a new Iceberg schema builder
 * Returns a pointer to the schema or null on error
 */
struct IcebergSchema *iceberg_schema_new(void);

/**
 * Add a long field to the schema
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
bool iceberg_schema_add_long_field(struct IcebergSchema *schema,
                                   uint32_t id,
                                   const char *name,
                                   bool required);

/**
 * Add an int field to the schema
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
bool iceberg_schema_add_int_field(struct IcebergSchema *schema,
                                  uint32_t id,
                                  const char *name,
                                  bool required);

/**
 * Add a date field to the schema
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
bool iceberg_schema_add_date_field(struct IcebergSchema *schema,
                                   uint32_t id,
                                   const char *name,
                                   bool required);

/**
 * Free an Iceberg schema
 */
void iceberg_schema_free(struct IcebergSchema *schema);

/**
 * Create a new Iceberg partition spec
 * Returns a pointer to the partition spec or null on error
 */
struct IcebergPartitionSpec *iceberg_partition_spec_new(void);

/**
 * Add a day partition field to the partition spec
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
bool iceberg_partition_spec_add_day_field(struct IcebergPartitionSpec *spec,
                                          uint32_t source_id,
                                          uint32_t field_id,
                                          const char *name);

/**
 * Free an Iceberg partition spec
 */
void iceberg_partition_spec_free(struct IcebergPartitionSpec *spec);

/**
 * Create a new Iceberg table
 * Returns a pointer to the table or null on error
 */
struct IcebergTable *iceberg_table_create(const char *name,
                                          const char *location,
                                          struct IcebergSchema *schema,
                                          struct IcebergPartitionSpec *partition_spec,
                                          struct IcebergCatalog *catalog,
                                          const char *namespace_name);

/**
 * Free an Iceberg table
 */
void iceberg_table_free(struct IcebergTable *table);

/**
 * Register an Iceberg table with the DataFusion context
 * Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
 */
int datafusion_register_iceberg_table(struct DataFusionContext *ctx,
                                      const char *table_name,
                                      struct IcebergTable *table);
