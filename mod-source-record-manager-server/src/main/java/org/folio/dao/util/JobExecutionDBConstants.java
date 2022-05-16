package org.folio.dao.util;

public final class JobExecutionDBConstants {

  public static final String ID_FIELD = "id";
  public static final String HRID_FIELD = "hrid";
  public static final String PARENT_ID_FIELD = "parent_job_id";
  public static final String SUBORDINATION_TYPE_FIELD = "subordination_type";
  public static final String JOB_PROFILE_ID_FIELD = "job_profile_id";
  public static final String JOB_PROFILE_NAME_FIELD = "job_profile_name";
  public static final String JOB_PROFILE_HIDDEN_FIELD = "job_profile_hidden";
  public static final String JOB_PROFILE_DATA_TYPE_FIELD = "job_profile_data_type";
  public static final String PROFILE_SNAPSHOT_WRAPPER_FIELD = "job_profile_snapshot_wrapper";
  public static final String SOURCE_PATH_FIELD = "source_path";
  public static final String FILE_NAME_FIELD = "file_name";
  public static final String PROGRESS_CURRENT_FIELD = "progress_current";
  public static final String PROGRESS_TOTAL_FIELD = "progress_total";
  public static final String STARTED_DATE_FIELD = "started_date";
  public static final String COMPLETED_DATE_FIELD = "completed_date";
  public static final String STATUS_FIELD = "status";
  public static final String UI_STATUS_FIELD = "ui_status";
  public static final String ERROR_STATUS_FIELD = "error_status";
  public static final String JOB_USER_FIRST_NAME_FIELD = "job_user_first_name";
  public static final String JOB_USER_LAST_NAME_FIELD = "job_user_last_name";
  public static final String USER_ID_FIELD = "user_id";
  public static final String TOTAL_COUNT_FIELD = "total_count";
  public static final String CURRENTLY_PROCESSED_FIELD = "currently_processed";
  public static final String TOTAL_FIELD = "total";
  public static final String IS_DELETED_FIELD = "is_deleted";

  public static final String GET_BY_ID_SQL = "SELECT * FROM %s WHERE id = $1 AND is_deleted = false";
  public static final String UPDATE_BY_IDS_SQL = "UPDATE ${tenantName}.${tableName} SET ${setFieldName} = ${setFieldValue} WHERE ${setConditionalFieldName} IN ('${setConditionalFieldValues}') RETURNING ${returningFieldNames}";

  public static final String INSERT_SQL =
    "INSERT INTO %s.%s (id, hrid, parent_job_id, subordination_type, source_path, file_name, " +
    "progress_current, progress_total, started_date, completed_date, status, ui_status, error_status, job_user_first_name, " +
    "job_user_last_name, user_id, job_profile_id, job_profile_name, job_profile_data_type, job_profile_snapshot_wrapper, "
      + "job_profile_hidden) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)";

  public static final String UPDATE_SQL =
    "UPDATE %s " +
    "SET id = $1, hrid = $2, parent_job_id = $3, subordination_type = $4, source_path = $5, file_name = $6, " +
    "progress_current = $7, progress_total = $8, started_date = $9, completed_date = $10, " +
    "status = $11, ui_status = $12, error_status = $13, job_user_first_name = $14, job_user_last_name = $15, " +
    "user_id = $16, job_profile_id = $17, job_profile_name = $18, job_profile_data_type = $19, " +
    "job_profile_snapshot_wrapper = $20, job_profile_hidden = $21" +
    "WHERE id = $1";

  public static final String GET_CHILDREN_JOBS_BY_PARENT_ID_SQL =
    "WITH cte AS (SELECT count(*) AS total_count FROM %s " +
    "WHERE parent_job_id = $1 AND subordination_type = 'CHILD' AND is_deleted = false) " +
    "SELECT j.*, cte.*, p.total_records_count total, " +
    "p.succeeded_records_count + p.error_records_count currently_processed " +
    "FROM %s j " +
    "LEFT JOIN %s p ON  j.id = p.job_execution_id " +
    "LEFT JOIN cte ON true " +
    "WHERE parent_job_id = $1 AND subordination_type = 'CHILD' AND is_deleted = false " +
    "LIMIT $2 OFFSET $3";

  public static final String GET_JOBS_NOT_PARENT_SQL =
    "WITH cte AS (SELECT count(*) AS total_count FROM %s " +
    "WHERE subordination_type <> 'PARENT_MULTIPLE' AND %s) " +
    "SELECT j.*, cte.*, p.total_records_count total, " +
    "p.succeeded_records_count + p.error_records_count currently_processed " +
    "FROM %s j " +
    "LEFT JOIN %s p ON  j.id = p.job_execution_id " +
    "LEFT JOIN cte ON true " +
    "WHERE subordination_type <> 'PARENT_MULTIPLE' AND %s " +
    "%s " +
    "LIMIT $1 OFFSET $2";

  private JobExecutionDBConstants() {
  }
}
