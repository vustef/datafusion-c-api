use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;

use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use arrow::array::RecordBatch;
use arrow::util::pretty;

// Opaque handles for C API
pub struct DataFusionContext {
    ctx: SessionContext,
    runtime: tokio::runtime::Runtime,
}

pub struct DataFusionResult {
    batches: Vec<RecordBatch>,
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
