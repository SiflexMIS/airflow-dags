from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.utils.dates import days_ago
import duckdb
import os

# MinIO 및 S3 관련 설정
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
SOURCE_BUCKET = "sivprod"
DESTINATION_BUCKET = "siv-parquet"

# DuckDB 쿼리 리스트 (각 쿼리를 별도 함수로 정의)
DUCKDB_QUERIES = {
  "wip_move_transactions": {
      "query": """
          COPY (
              SELECT
                  FROM_OP_SEQ_NO,    
                  JOB_NO,
                  ITEM_UOM_CODE,
                  UOM_QTY,
                  MTX_UOM_CODE,
                  FACTOR_VALUE1,
                  MTX_UOM_QTY1,
                  FACTOR_VALUE2,
                  MTX_UOM_QTY2,
                  LAST_UPDATED_BY,
                  TRY_CAST(EXTEND_DATE AS DATE) as EXTEND_DATE,
                  TRY_CAST(EXTEND_DATE_06 AS DATE) as EXTEND_DATE_06,
                  MOVE_TRX_TYPE,
                  TRY_CAST(MOVE_TRX_DATE AS DATETIME) as MOVE_TRX_DATE,
                  MOVE_TRX_ID,
                  BOM_ITEM_ID,
                  JOB_ID,
                  FROM_OPERATION_ID,
                  FROM_RESOURCE_ID,
                  TO_OP_SEQ_NO,
                  SOB_ID,
                  ORG_ID,
                  DATE_TRUNC('year',TRY_CAST(EXTEND_DATE AS DATE)) AS year,
                  DATE_TRUNC('month',TRY_CAST(EXTEND_DATE AS DATE)) AS month
              FROM read_parquet('s3://{source_bucket}/APPS/WIP_MOVE_TRANSACTIONS/*.parquet',
                  filename=true,
                  hive_partitioning=0,
                  union_by_name=1,
                  s3_endpoint='{minio_endpoint}',
                  s3_access_key_id='{minio_access_key}',
                  s3_secret_access_key='{minio_secret_key}',
                  s3_use_ssl='False')             
          ) TO 's3://{destination_bucket}/wip_move_transactions_monthly_large.parquet'
          (FORMAT 'parquet',
           PARTITION_BY (year, month),
           ROW_GROUP_SIZE 268435456,
           CODEC 'zstd', OVERWRITE_OR_IGNORE);
      """,
      "source_prefix": "APPS/WIP_MOVE_TRANSACTIONS/",
      "destination_file": "wip_move_transactions_monthly_large.parquet"
  },
    "wip_move_trx_eqp": {
        "query": """
            COPY (
                SELECT
                    TRX_EQP_ID,
                    RUN_END_MOVE_TRX_ID,
                    CANCEL_RUN_END_MOVE_TRX_ID,
                    TRY_CAST(RUN_END_DATE AS DATETIME) AS RUN_END_DATE,
                    EQUIPMENT_ID,
                    JOB_ID,
                    OPERATION_SEQ_NO,
                    OPERATION_ID,
                    DATE_TRUNC('year',TRY_CAST(RUN_END_DATE AS DATETIME)) AS year,
                    DATE_TRUNC('month',TRY_CAST(RUN_END_DATE AS DATETIME)) AS month
                FROM read_parquet('s3://{source_bucket}/APPS/WIP_MOVE_TRX_EQP/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/wip_move_trx_eqp_monthly_large.parquet'
            (FORMAT 'parquet',
             PARTITION_BY (year, month),
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/WIP_MOVE_TRX_EQP/",
        "destination_file": "wip_move_trx_eqp_monthly_large.parquet"
    },
    "sdm_standard_equipment": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/SDM_STANDARD_EQUIPMENT/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/SDM_STANDARD_EQUIPMENT.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/SDM_STANDARD_EQUIPMENT/",
        "destination_file": "SDM_STANDARD_EQUIPMENT.parquet"
    },
  "inv_item_section": {
      "query": """
          COPY (
              SELECT *
              FROM read_parquet('s3://{source_bucket}/APPS/INV_ITEM_SECTION/*.parquet',
                  filename=true,
                  hive_partitioning=0,
                  union_by_name=1,
                  s3_endpoint='{minio_endpoint}',
                  s3_access_key_id='{minio_access_key}',
                  s3_secret_access_key='{minio_secret_key}',
                  s3_use_ssl='False')
          ) TO 's3://{destination_bucket}/INV_ITEM_SECTION.parquet'
          (FORMAT 'parquet',
           ROW_GROUP_SIZE 268435456,
           CODEC 'zstd', OVERWRITE_OR_IGNORE);
      """,
      "source_prefix": "APPS/INV_ITEM_SECTION/",
      "destination_file": "INV_ITEM_SECTION.parquet"
  },
  "sdm_item_spec": {
      "query": """
          COPY (
              SELECT *
              FROM read_parquet('s3://{source_bucket}/APPS/SDM_ITEM_SPEC/*.parquet',
                  filename=true,
                  hive_partitioning=0,
                  union_by_name=1,
                  s3_endpoint='{minio_endpoint}',
                  s3_access_key_id='{minio_access_key}',
                  s3_secret_access_key='{minio_secret_key}',
                  s3_use_ssl='False')
          ) TO 's3://{destination_bucket}/SDM_ITEM_SPEC.parquet'
          (FORMAT 'parquet',
           ROW_GROUP_SIZE 268435456,
           CODEC 'zstd', OVERWRITE_OR_IGNORE);
      """,
      "source_prefix": "APPS/SDM_ITEM_SPEC/",
      "destination_file": "SDM_ITEM_SPEC.parquet"
  },
  "sdm_item_structure": {
      "query": """
          COPY (
              SELECT *
              FROM read_parquet('s3://{source_bucket}/APPS/SDM_ITEM_STRUCTURE/*.parquet',
                  filename=true,
                  hive_partitioning=0,
                  union_by_name=1,
                  s3_endpoint='{minio_endpoint}',
                  s3_access_key_id='{minio_access_key}',
                  s3_secret_access_key='{minio_secret_key}',
                  s3_use_ssl='False')
          ) TO 's3://{destination_bucket}/SDM_ITEM_STRUCTURE.parquet'
          (FORMAT 'parquet',
           ROW_GROUP_SIZE 268435456,
           CODEC 'zstd', OVERWRITE_OR_IGNORE);
      """,
      "source_prefix": "APPS/SDM_ITEM_STRUCTURE/",
      "destination_file": "SDM_ITEM_STRUCTURE.parquet"
  },
    "sdm_standard_operation": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/SDM_STANDARD_OPERATION/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/SDM_STANDARD_OPERATION.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/SDM_STANDARD_OPERATION/",
        "destination_file": "SDM_STANDARD_OPERATION.parquet"
    },
    "inv_item_master": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/INV_ITEM_MASTER/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/INV_ITEM_MASTER.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/INV_ITEM_MASTER/",
        "destination_file": "INV_ITEM_MASTER.parquet"
    },
    "sdm_item_revision": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/SDM_ITEM_REVISION/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/SDM_ITEM_REVISION.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/SDM_ITEM_REVISION/",
        "destination_file": "SDM_ITEM_REVISION.parquet"
    },
    "sdm_standard_workcenter": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/SDM_STANDARD_WORKCENTER/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/SDM_STANDARD_WORKCENTER.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/SDM_STANDARD_WORKCENTER/",
        "destination_file": "SDM_STANDARD_WORKCENTER.parquet"
    },
    "sdm_standard_resource": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/SDM_STANDARD_RESOURCE/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/SDM_ITEM_STRUCTURE.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/SDM_STANDARD_RESOURCE/",
        "destination_file": "SDM_ITEM_STRUCTURE.parquet"
    },
    "eapp_user": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/EAPP_USER/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/EAPP_USER.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/EAPP_USER/",
        "destination_file": "EAPP_USER.parquet"
    },
    "eapp_lookup_entry": {
        "query": """
            COPY (
                SELECT *
                FROM read_parquet('s3://{source_bucket}/APPS/EAPP_LOOKUP_ENTRY/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/EAPP_LOOKUP_ENTRY.parquet'
            (FORMAT 'parquet',
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/EAPP_LOOKUP_ENTRY/",
        "destination_file": "EAPP_LOOKUP_ENTRY.parquet"
    },
  "wip_operations": {
      "query": """
          COPY (
              SELECT
                  JOB_ID,
                  JOB_NO,
                  BOM_ITEM_ID,
                  OPERATION_SEQ_NO,
                  OPERATION_ID,
                  WORKCENTER_ID,
                  RESOURCE_ID,
                  RTR_SHEET,
                  WORK_WIDTH,
                  TRY_CAST(RUN_START_DATE AS DATETIME) AS RUN_START_DATE,
                  TRY_CAST(RUN_END_DATE AS DATETIME) AS RUN_END_DATE,
                  TRY_CAST(TOMOVE_DATE AS DATETIME) AS TOMOVE_DATE,
                  DATE_TRUNC('year',TRY_CAST(TOMOVE_DATE AS DATETIME)) AS year,
                  DATE_TRUNC('month',TRY_CAST(TOMOVE_DATE AS DATETIME)) AS month
              FROM read_parquet('s3://{source_bucket}/APPS/WIP_OPERATIONS/*.parquet',
                  filename=true,
                  hive_partitioning=0,
                  union_by_name=1,
                  s3_endpoint='{minio_endpoint}',
                  s3_access_key_id='{minio_access_key}',
                  s3_secret_access_key='{minio_secret_key}',
                  s3_use_ssl='False')
          ) TO 's3://{destination_bucket}/WIP_OPERATIONS.parquet'
          (FORMAT 'parquet',
           ROW_GROUP_SIZE 268435456,
           PARTITION_BY (year, month),
           CODEC 'zstd', OVERWRITE_OR_IGNORE);
      """,
      "source_prefix": "APPS/WIP_OPERATIONS/",
      "destination_file": "WIP_OPERATIONS.parquet"
  },
    "wip_job_entities": {
        "query": """
            COPY (
                SELECT
                    JOB_ID,
                    JOB_NO,
                    BOM_ITEM_ID,
                    INVENTORY_ITEM_ID,
                    JOB_STATUS_CODE,
                    TRY_CAST(JOB_RELEASE_DATE AS DATETIME) AS JOB_RELEASE_DATE,
                    TRY_CAST(WIP_COMPLETE_EXTEND_DATE_06 AS DATETIME) AS WIP_COMPLETE_EXTEND_DATE_06,
                    WEEK_NUM,
                    SOB_ID,
                    ORG_ID,
                    MM_UOM_FACTOR,
                    PNL_UOM_FACTOR,
                    MM_UOM_FACTOR,
                    SHEET_UOM_FACTOR,
                    ARRAY_UOM_FACTOR,
                    PCS_PNL,
                    PCS_MM,
                    ASSEMBLE_SET_PNL,
                    RELEASE_QTY,
                    ONHAND_QTY,
                    SCRAP_QTY,
                    DATE_TRUNC('year',TRY_CAST(JOB_RELEASE_DATE AS DATETIME)) AS year,
                    DATE_TRUNC('month',TRY_CAST(JOB_RELEASE_DATE AS DATETIME)) AS month
                FROM read_parquet('s3://{source_bucket}/APPS/WIP_JOB_ENTITIES/*.parquet',
                    filename=true,
                    hive_partitioning=0,
                    union_by_name=1,
                    s3_endpoint='{minio_endpoint}',
                    s3_access_key_id='{minio_access_key}',
                    s3_secret_access_key='{minio_secret_key}',
                    s3_use_ssl='False')
            ) TO 's3://{destination_bucket}/WIP_JOB_ENTITIES.parquet'
            (FORMAT 'parquet',
             PARTITION_BY (year, month),
             ROW_GROUP_SIZE 268435456,
             CODEC 'zstd', OVERWRITE_OR_IGNORE);
        """,
        "source_prefix": "APPS/WIP_JOB_ENTITIES/",
        "destination_file": "WIP_JOB_ENTITIES.parquet"
    }
}

def execute_duckdb_query(query, source_bucket, destination_bucket, minio_endpoint, minio_access_key, minio_secret_key):
    """DuckDB 쿼리를 실행합니다."""
    conn = duckdb.connect()
    conn.execute(
        f"""
        INSTALL httpfs;
        INSTALL aws;
        LOAD httpfs;
        LOAD aws;

        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{minio_access_key}';
        SET s3_secret_access_key='{minio_secret_key}';
        SET s3_url_style='path';
        SET s3_use_ssl='False';
        """
    )
    conn.execute(query.format(
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key
    ))
    conn.close()

with DAG(
    dag_id="process_minio_parquet_multiple_queries",
    schedule_interval='@hourly', 
    start_date=days_ago(1),
    catchup=False,
    tags=["minio", "parquet", "duckdb"],
) as dag:
    
    # 작업 정의
    for table_name, query_info in DUCKDB_QUERIES.items():
      
        # 파티셔닝 된 테이블은 삭제 진행
        # if table_name in ["wip_move_transactions", "wip_move_trx_eqp", "wip_operations", "wip_job_entities"]:
        #     delete_files = S3DeleteObjectsOperator(
        #         task_id=f"delete_previous_files_{table_name}",
        #         bucket=DESTINATION_BUCKET,
        #         prefix=f"{query_info['destination_file']}/",
        #         aws_conn_id="aws_default",
        #     )

        run_query = PythonOperator(
            task_id=f"run_query_{table_name}",
            python_callable=execute_duckdb_query,
            op_kwargs={
                "query": query_info["query"],
                "source_bucket": SOURCE_BUCKET,
                "destination_bucket": DESTINATION_BUCKET,
                "minio_endpoint": MINIO_ENDPOINT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
            },
        )
        
        # 파티셔닝 된 테이블만 의존성 추가
        # if table_name in ["wip_move_transactions", "wip_move_trx_eqp", "wip_operations", "wip_job_entities"]:
        #   delete_files >> run_query
        # else:
        #   run_query
