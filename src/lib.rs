use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use datafusion_iceberg::DataFusionTable;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty;
use iceberg_rust::{
    catalog::Catalog,
    object_store::ObjectStoreBuilder,
    spec::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, Type},
    },
    table::Table,
};
use iceberg_sql_catalog::SqlCatalog;

// Opaque handles for C API
pub struct DataFusionContext {
    ctx: SessionContext,
    runtime: tokio::runtime::Runtime,
}

pub struct DataFusionResult {
    batches: Vec<RecordBatch>,
}

pub struct IcebergCatalog {
    catalog: Arc<dyn Catalog>,
    runtime: tokio::runtime::Runtime,
}

pub struct IcebergSchema {
    builder: iceberg_rust::spec::schema::SchemaBuilder,
}

pub struct IcebergPartitionSpec {
    builder: iceberg_rust::spec::partition::PartitionSpecBuilder,
}

pub struct IcebergTable {
    table: Arc<DataFusionTable>,
}

// Error codes
pub const DATAFUSION_OK: c_int = 0;
pub const DATAFUSION_ERROR: c_int = -1;

/// Create a new DataFusion context
/// Returns a pointer to the context or null on error
#[no_mangle]
pub extern "C" fn datafusion_context_new() -> *mut DataFusionContext {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };
    
    let ctx = SessionContext::new();
    
    let df_ctx = Box::new(DataFusionContext { ctx, runtime });
    Box::into_raw(df_ctx)
}

/// Free a DataFusion context
#[no_mangle]
pub extern "C" fn datafusion_context_free(ctx: *mut DataFusionContext) {
    if !ctx.is_null() {
        unsafe {
            let _ = Box::from_raw(ctx);
        }
    }
}

/// Register a CSV file with the context
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn datafusion_register_csv(
    ctx: *mut DataFusionContext,
    table_name: *const c_char,
    file_path: *const c_char,
) -> c_int {
    if ctx.is_null() || table_name.is_null() || file_path.is_null() {
        return DATAFUSION_ERROR;
    }

    let ctx = unsafe { &mut *ctx };
    
    let table_name = match unsafe { CStr::from_ptr(table_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return DATAFUSION_ERROR,
    };
    
    let file_path = match unsafe { CStr::from_ptr(file_path) }.to_str() {
        Ok(s) => s,
        Err(_) => return DATAFUSION_ERROR,
    };

    match ctx.runtime.block_on(async {
        ctx.ctx.register_csv(table_name, file_path, CsvReadOptions::new()).await
    }) {
        Ok(_) => DATAFUSION_OK,
        Err(_) => DATAFUSION_ERROR,
    }
}

/// Execute a SQL query
/// Returns a pointer to the result or null on error
#[no_mangle]
pub extern "C" fn datafusion_sql(
    ctx: *mut DataFusionContext,
    sql: *const c_char,
) -> *mut DataFusionResult {
    if ctx.is_null() || sql.is_null() {
        return ptr::null_mut();
    }

    let ctx = unsafe { &mut *ctx };
    
    let sql_str = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let batches = match ctx.runtime.block_on(async {
        let df = ctx.ctx.sql(sql_str).await?;
        df.collect().await
    }) {
        Ok(batches) => batches,
        Err(_) => return ptr::null_mut(),
    };

    let result = Box::new(DataFusionResult { batches });
    Box::into_raw(result)
}

/// Get the number of batches in a result
#[no_mangle]
pub extern "C" fn datafusion_result_batch_count(result: *const DataFusionResult) -> c_int {
    if result.is_null() {
        return 0;
    }
    
    let result = unsafe { &*result };
    result.batches.len() as c_int
}

/// Get the number of rows in a specific batch
#[no_mangle]
pub extern "C" fn datafusion_result_batch_num_rows(
    result: *const DataFusionResult,
    batch_index: c_int,
) -> c_int {
    if result.is_null() || batch_index < 0 {
        return 0;
    }
    
    let result = unsafe { &*result };
    let index = batch_index as usize;
    
    if index >= result.batches.len() {
        return 0;
    }
    
    result.batches[index].num_rows() as c_int
}

/// Get the number of columns in a specific batch
#[no_mangle]
pub extern "C" fn datafusion_result_batch_num_columns(
    result: *const DataFusionResult,
    batch_index: c_int,
) -> c_int {
    if result.is_null() || batch_index < 0 {
        return 0;
    }
    
    let result = unsafe { &*result };
    let index = batch_index as usize;
    
    if index >= result.batches.len() {
        return 0;
    }
    
    result.batches[index].num_columns() as c_int
}

/// Print a result as a formatted table (for debugging)
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn datafusion_result_print(result: *const DataFusionResult) -> c_int {
    if result.is_null() {
        return DATAFUSION_ERROR;
    }
    
    let result = unsafe { &*result };
    
    match pretty::print_batches(&result.batches) {
        Ok(_) => DATAFUSION_OK,
        Err(_) => DATAFUSION_ERROR,
    }
}

/// Free a DataFusion result
#[no_mangle]
pub extern "C" fn datafusion_result_free(result: *mut DataFusionResult) {
    if !result.is_null() {
        unsafe {
            let _ = Box::from_raw(result);
        }
    }
}

/// Get last error message (simplified for this example)
#[no_mangle]
pub extern "C" fn datafusion_get_last_error() -> *const c_char {
    // In a real implementation, you'd want to store error messages in thread-local storage
    b"DataFusion error occurred\0".as_ptr() as *const c_char
}

// Iceberg-related functions

/// Create a new SQL catalog for Iceberg
/// Returns a pointer to the catalog or null on error
#[no_mangle]
pub extern "C" fn iceberg_catalog_new_sql(database_url: *const c_char, name: *const c_char) -> *mut IcebergCatalog {
    if database_url.is_null() || name.is_null() {
        return ptr::null_mut();
    }

    let database_url = match unsafe { CStr::from_ptr(database_url) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };

    let catalog = match runtime.block_on(async {
        let object_store = ObjectStoreBuilder::memory();
        SqlCatalog::new(database_url, name, object_store).await
    }) {
        Ok(catalog) => Arc::new(catalog) as Arc<dyn Catalog>,
        Err(_) => return ptr::null_mut(),
    };

    let iceberg_catalog = Box::new(IcebergCatalog { catalog, runtime });
    Box::into_raw(iceberg_catalog)
}

/// Free an Iceberg catalog  
#[no_mangle]
pub extern "C" fn iceberg_catalog_free(catalog: *mut IcebergCatalog) {
    if !catalog.is_null() {
        unsafe {
            let _ = Box::from_raw(catalog);
        }
    }
}

/// Create a new Iceberg schema builder
/// Returns a pointer to the schema or null on error
#[no_mangle]
pub extern "C" fn iceberg_schema_new() -> *mut IcebergSchema {
    let builder = Schema::builder();
    let schema = Box::new(IcebergSchema { builder });
    Box::into_raw(schema)
}

/// Add a long field to the schema
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn iceberg_schema_add_long_field(
    schema: *mut IcebergSchema,
    id: u32,
    name: *const c_char,
    required: bool,
) -> bool {
    if schema.is_null() || name.is_null() {
        return false;
    }

    let schema = unsafe { &mut *schema };
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };

    schema.builder.with_struct_field(StructField {
        id: id as i32,
        name: name.to_string(),
        required,
        field_type: Type::Primitive(PrimitiveType::Long),
        doc: None,
    });

    true
}

/// Add an int field to the schema
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn iceberg_schema_add_int_field(
    schema: *mut IcebergSchema,
    id: u32,
    name: *const c_char,
    required: bool,
) -> bool {
    if schema.is_null() || name.is_null() {
        return false;
    }

    let schema = unsafe { &mut *schema };
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };

    schema.builder.with_struct_field(StructField {
        id: id as i32,
        name: name.to_string(),
        required,
        field_type: Type::Primitive(PrimitiveType::Int),
        doc: None,
    });

    true
}

/// Add a date field to the schema
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn iceberg_schema_add_date_field(
    schema: *mut IcebergSchema,
    id: u32,
    name: *const c_char,
    required: bool,
) -> bool {
    if schema.is_null() || name.is_null() {
        return false;
    }

    let schema = unsafe { &mut *schema };
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };

    schema.builder.with_struct_field(StructField {
        id: id as i32,
        name: name.to_string(),
        required,
        field_type: Type::Primitive(PrimitiveType::Date),
        doc: None,
    });

    true
}

/// Free an Iceberg schema
#[no_mangle]
pub extern "C" fn iceberg_schema_free(schema: *mut IcebergSchema) {
    if !schema.is_null() {
        unsafe {
            let _ = Box::from_raw(schema);
        }
    }
}

/// Create a new Iceberg partition spec
/// Returns a pointer to the partition spec or null on error
#[no_mangle]
pub extern "C" fn iceberg_partition_spec_new() -> *mut IcebergPartitionSpec {
    let builder = PartitionSpec::builder();
    let partition_spec = Box::new(IcebergPartitionSpec { builder });
    Box::into_raw(partition_spec)
}

/// Add a day partition field to the partition spec
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn iceberg_partition_spec_add_day_field(
    spec: *mut IcebergPartitionSpec,
    source_id: u32,
    field_id: u32,
    name: *const c_char,
) -> bool {
    if spec.is_null() || name.is_null() {
        return false;
    }

    let spec = unsafe { &mut *spec };
    let name = unsafe { CStr::from_ptr(name) };
    let name = match name.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };

    spec.builder.with_partition_field(PartitionField::new(
        source_id as i32,
        field_id as i32,
        name,
        Transform::Day,
    ));

    true
}

/// Free an Iceberg partition spec
#[no_mangle]
pub extern "C" fn iceberg_partition_spec_free(spec: *mut IcebergPartitionSpec) {
    if !spec.is_null() {
        unsafe {
            let _ = Box::from_raw(spec);
        }
    }
}

/// Create a new Iceberg table
/// Returns a pointer to the table or null on error
#[no_mangle]
pub extern "C" fn iceberg_table_create(
    name: *const c_char,
    location: *const c_char,
    schema: *mut IcebergSchema,
    partition_spec: *mut IcebergPartitionSpec,
    catalog: *mut IcebergCatalog,
    namespace_name: *const c_char,
) -> *mut IcebergTable {
    if name.is_null() || location.is_null() || schema.is_null() || 
       partition_spec.is_null() || catalog.is_null() || namespace_name.is_null() {
        return ptr::null_mut();
    }

    let name = match unsafe { CStr::from_ptr(name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let location = match unsafe { CStr::from_ptr(location) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let namespace_name = match unsafe { CStr::from_ptr(namespace_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    // Take ownership of the builders (this consumes them)
    let mut schema_box = unsafe { Box::from_raw(schema) };
    let partition_spec_box = unsafe { Box::from_raw(partition_spec) };
    let catalog = unsafe { &mut *catalog };

    // Build the schema and partition spec (this consumes the builders)
    let built_schema = match schema_box.builder.build() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let built_partition_spec = match partition_spec_box.builder.build() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    // Note: schema_box and partition_spec_box are now consumed and should not be freed by Julia finalizers
    // This fixes the double-free issue

    let table = match catalog.runtime.block_on(async {
        Table::builder()
            .with_name(name)
            .with_location(location)
            .with_schema(built_schema)
            .with_partition_spec(built_partition_spec)
            .build(&[namespace_name.to_owned()], catalog.catalog.clone())
            .await
    }) {
        Ok(table) => Arc::new(DataFusionTable::from(table)),
        Err(_) => return ptr::null_mut(),
    };

    let iceberg_table = Box::new(IcebergTable { table });
    Box::into_raw(iceberg_table)
}

/// Free an Iceberg table
#[no_mangle]
pub extern "C" fn iceberg_table_free(table: *mut IcebergTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

/// Register an Iceberg table with the DataFusion context
/// Returns DATAFUSION_OK on success, DATAFUSION_ERROR on failure
#[no_mangle]
pub extern "C" fn datafusion_register_iceberg_table(
    ctx: *mut DataFusionContext,
    table_name: *const c_char,
    table: *mut IcebergTable,
) -> c_int {
    if ctx.is_null() || table_name.is_null() || table.is_null() {
        return DATAFUSION_ERROR;
    }

    let ctx = unsafe { &mut *ctx };
    let table = unsafe { &*table };
    
    let table_name = match unsafe { CStr::from_ptr(table_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return DATAFUSION_ERROR,
    };

    match ctx.ctx.register_table(table_name, table.table.clone()) {
        Ok(_) => DATAFUSION_OK,
        Err(_) => DATAFUSION_ERROR,
    }
}
