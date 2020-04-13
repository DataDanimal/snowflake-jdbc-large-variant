/*
  The following sample query will query the logs stored post-execution.
  Can be used to create a view and aggregate from there
*/
select
    l.log_seq,
    replace(log.value :bytes_per_sec, ',', '') :: double bytes_per_sec,
    replace(log.value :exec_in_secs, ',', '') :: double exec_in_secs,
    replace(log.value :mb_per_sec, ',', '') :: double mb_per_sec,
    replace(log.value :gb_per_sec, ',', '') :: double gb_per_sec,
    replace(log.value :rows_per_sec, ',', '') :: double rows_per_sec,
    replace(log.value :total_inserts, ',', '') :: double total_inserts,
    replace(log.value :total_size, ',', '') :: double total_size,
    replace(log.value :total_size_gb, ',', '') :: double total_size_gb,
    replace(log.value :total_size_mb, ',', '') :: double total_size_mb,
    replace(dtl.value :log_bytes_per_sec, ',', '') :: double log_bytes_per_sec,
    replace(dtl.value :log_gb_per_sec, ',', '') :: double log_gb_per_sec,
    replace(dtl.value :log_interval, ',', '') :: double log_interval,
    replace(dtl.value :log_mb_per_sec, ',', '') :: double log_mb_per_sec,
    replace(dtl.value :log_row, ',', '') :: double log_row,
    replace(dtl.value :log_rows_per_sec, ',', '') :: double log_rows_per_sec,
    replace(dtl.value :log_total_size, ',', '') :: double log_total_size,
    replace(dtl.value :log_total_size_gb, ',', '') :: double log_total_size_gb,
    replace(dtl.value :log_total_size_mb, ',', '') :: double log_total_size_mb,
    dtl.value :log_ts :: timestamp log_ts,
    log.value :prop_total_inserts :: number prop_total_inserts,
    log.value :start_ts :: timestamp start_ts,
    log.value :prop_account :: string prop_account,
    log.value :prop_auto_commit :: boolean prop_auto_commit,
    log.value :prop_batch_size :: number prop_batch_size,
    log.value :prop_db :: string prop_db,
    log.value :prop_drop_objects :: boolean prop_drop_objects,
    log.value :prop_json_obj_size :: number prop_json_obj_size,
    log.value :prop_log_results :: boolean prop_log_results,
    log.value :prop_reset_logs :: boolean prop_reset_logs,
    log.value :prop_role :: string prop_role,
    log.value :prop_use_variant :: string prop_use_variant,
    log.value :prop_user :: string prop_user,
    log.value :prop_warehouse :: string prop_warehouse,
    prop_total_inserts / 1000 as prop_total_inserts_K,
    prop_batch_size / 1000 as prop_batch_size_k,
    prop_total_inserts || '_' || prop_batch_size || '_' || prop_json_obj_size || '_' as log_id,
    prop_total_inserts_K :: integer || 'K (' || (prop_total_inserts_K / prop_batch_size_k) :: decimal (10, 1) || ' Batches of ' || prop_batch_size_k :: integer || 'K)' as log_short_id
from
    large_insert_log l,
    lateral flatten(input => l.log_msg :log) log,
    lateral flatten(input => log.value :log_result) dtl
order by
    l.log_seq,
    log_ts;