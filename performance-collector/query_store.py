class QueryStore:
    queries = {"dm_os_wait_stats": {'sql_text': """
                                SELECT wait_type ,
                                       waiting_tasks_count ,
                                       wait_time_ms ,
                                       max_wait_time_ms ,
                                       signal_wait_time_ms
                                FROM sys.dm_os_wait_stats""",
                                    'key_col': 'wait_type'
                                    },

               "query_stats": {'sql_text': """
    SELECT statement_text,
       execution_count ,
       total_worker_time / 1000000 as total_worker_seconds,
       min_worker_time / 1000000 as min_worker_seconds,
       max_worker_time / 1000000 as max_worker_seconds,
       total_physical_reads ,
       min_physical_reads ,
       max_physical_reads ,
       total_logical_writes ,
       min_logical_writes ,
       max_logical_writes ,
       total_logical_reads ,
       min_logical_reads ,
       max_logical_reads ,
       total_clr_time ,
       min_clr_time ,
       max_clr_time ,
       total_elapsed_time / 1000000 as total_elapsed_seconds,
       min_elapsed_time / 1000000 as min_elapsed_seconds,
       max_elapsed_time / 1000000 as max_elapsed_seconds,
       total_rows ,
       min_rows ,
       max_rows
    FROM sys.dm_exec_query_stats
    CROSS APPLY (SELECT text,
                     SUBSTRING(text, statement_start_offset / 2 + 1,
                                    (CASE WHEN statement_end_offset = -1
                                            THEN LEN(CONVERT(nvarchar(MAX),text)) * 2
                                            ELSE statement_end_offset
                                        END - statement_start_offset) / 2)
              FROM sys.dm_exec_sql_text(sql_handle)

             ) AS query(raw_sql, statement_text)
                                         """,
                               'key_col': 'statement_text'
                               },

               "dm_io_virtual_file_stats": {'sql_text': """
    SELECT db_name(database_id) + '(' + CAST(file_id AS VARCHAR(10)) + ')' as file_id,
           num_of_reads ,
           num_of_bytes_read ,
           io_stall_read_ms ,
           io_stall_queued_read_ms ,
           num_of_writes ,
           num_of_bytes_written ,
           io_stall_write_ms ,
           io_stall_queued_write_ms ,
           io_stall ,
           size_on_disk_bytes
    FROM sys.dm_io_virtual_file_stats(NULL, NULL)
    WHERE database_id > 4
    """,
                                            'key_col': 'file_id'
                                            },
               "partition_stats": {'sql_text': """
                SET NOCOUNT ON;

                DECLARE @partition_stats table(
                    [partition_key]	VARCHAR(500),
                    [db_id] INT,
                    [partition_id] BIGINT,
                    [object_id]	INT,
                    [index_id]	INT,
                    [partition_number]	INT,
                    [in_row_data_page_count]	BIGINT,
                    [in_row_used_page_count]	BIGINT,
                    [in_row_reserved_page_count]	BIGINT,
                    [lob_used_page_count]	BIGINT,
                    [lob_reserved_page_count]	BIGINT,
                    [row_overflow_used_page_count]	BIGINT,
                    [row_overflow_reserved_page_count]	BIGINT,
                    [used_page_count]	BIGINT,
                    [reserved_page_count]	BIGINT,
                    [row_count]	BIGINT
                );

                INSERT INTO @partition_stats
                        ( partition_key ,
                          [db_id],
                          [partition_id],
                          object_id ,
                          index_id ,
                          partition_number ,
                          in_row_data_page_count ,
                          in_row_used_page_count ,
                          in_row_reserved_page_count ,
                          lob_used_page_count ,
                          lob_reserved_page_count ,
                          row_overflow_used_page_count ,
                          row_overflow_reserved_page_count ,
                          used_page_count ,
                          reserved_page_count ,
                          row_count
                        )
                EXEC sp_msforeachdb '
                    IF ''?''  NOT IN (''tempDB'',''model'',''msdb'')
                    BEGIN
                        SELECT
                           CONCAT(DB_ID(''?''), ''_'', partition_id) AS partition_key ,
                           DB_ID(''?'') as db_id,
                           partition_id,
                           object_id ,
                           index_id ,
                           partition_number ,
                           in_row_data_page_count ,
                           in_row_used_page_count ,
                           in_row_reserved_page_count ,
                           lob_used_page_count ,
                           lob_reserved_page_count ,
                           row_overflow_used_page_count ,
                           row_overflow_reserved_page_count ,
                           used_page_count ,
                           reserved_page_count ,
                           row_count
                        FROM sys.dm_db_partition_stats
                    END    ';
                SET NOCOUNT OFF;

                SELECT
                    [partition_key],
                    [db_id],
                    [object_id],
                    [index_id],
                    [partition_number],
                    [in_row_data_page_count],
                    [in_row_used_page_count],
                    [in_row_reserved_page_count],
                    [lob_used_page_count],
                    [lob_reserved_page_count],
                    [row_overflow_used_page_count],
                    [row_overflow_reserved_page_count],
                    [used_page_count],
                    [reserved_page_count],
                    [row_count]

                FROM @partition_stats;
               """, 'key_col': 'partition_key'
                                   },
               "active_sessions": {'sql_text': """
                SELECT session_id ,
                       cpu_time ,
                       memory_usage ,
                       total_scheduled_time ,
                       total_elapsed_time ,
                       last_request_duration_seconds = DATEDIFF(SECOND, last_request_start_time, last_request_end_time),
                       reads ,
                       writes ,
                       logical_reads ,
                       row_count
                FROM sys.dm_exec_sessions
                WHERE session_id > 50
               """, 'key_col': 'session_id'}
               }

    @staticmethod
    def get_query_text(query_name):
        return QueryStore.queries[query_name]['sql_text']

    @staticmethod
    def get_query_key_col(query_name):
        return QueryStore.queries[query_name]['key_col']

    @staticmethod
    def get_query_names(self):
        return self.queries.keys()
