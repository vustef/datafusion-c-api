#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define DATAFUSION_OK 0

#define DATAFUSION_ERROR -1

typedef struct DataFusionContext DataFusionContext;

typedef struct DataFusionResult DataFusionResult;

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
