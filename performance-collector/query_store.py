class QueryStore:
    queries = {"dm_os_wait_stats": {'sql_text': """
                                -- PerfCollector
                                SELECT wait_type ,
                                       waiting_tasks_count ,
                                       wait_time_ms ,
                                       max_wait_time_ms ,
                                       signal_wait_time_ms
                                FROM sys.dm_os_wait_stats
                                WHERE waiting_tasks_count + wait_time_ms + max_wait_time_ms + signal_wait_time_ms > 0
                                and [wait_type] NOT IN (
        N'BROKER_EVENTHANDLER',             N'BROKER_RECEIVE_WAITFOR',
        N'BROKER_TASK_STOP',                N'BROKER_TO_FLUSH',
        N'BROKER_TRANSMITTER',              N'CHECKPOINT_QUEUE',
        N'CHKPT',                           N'CLR_AUTO_EVENT',
        N'CLR_MANUAL_EVENT',                N'CLR_SEMAPHORE',
        N'DBMIRROR_DBM_EVENT',              N'DBMIRROR_EVENTS_QUEUE',
        N'DBMIRROR_WORKER_QUEUE',           N'DBMIRRORING_CMD',
        N'DIRTY_PAGE_POLL',                 N'DISPATCHER_QUEUE_SEMAPHORE',
        N'EXECSYNC',                        N'FSAGENT',
        N'FT_IFTS_SCHEDULER_IDLE_WAIT',     N'FT_IFTSHC_MUTEX',
        N'HADR_CLUSAPI_CALL',               N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        N'HADR_LOGCAPTURE_WAIT',            N'HADR_NOTIFICATION_DEQUEUE',
        N'HADR_TIMER_TASK',                 N'HADR_WORK_QUEUE',
        N'KSOURCE_WAKEUP',                  N'LAZYWRITER_SLEEP',
        N'LOGMGR_QUEUE',                    N'ONDEMAND_TASK_QUEUE',
        N'PWAIT_ALL_COMPONENTS_INITIALIZED',
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        N'QDS_SHUTDOWN_QUEUE',
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        N'REQUEST_FOR_DEADLOCK_SEARCH',     N'RESOURCE_QUEUE',
        N'SERVER_IDLE_CHECK',               N'SLEEP_BPOOL_FLUSH',
        N'SLEEP_DBSTARTUP',                 N'SLEEP_DCOMSTARTUP',
        N'SLEEP_MASTERDBREADY',             N'SLEEP_MASTERMDREADY',
        N'SLEEP_MASTERUPGRADED',            N'SLEEP_MSDBSTARTUP',
        N'SLEEP_SYSTEMTASK',                N'SLEEP_TASK',
        N'SLEEP_TEMPDBSTARTUP',             N'SNI_HTTP_ACCEPT',
        N'SP_SERVER_DIAGNOSTICS_SLEEP',     N'SQLTRACE_BUFFER_FLUSH',
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        N'SQLTRACE_WAIT_ENTRIES',           N'WAIT_FOR_RESULTS',
        N'WAITFOR',                         N'WAITFOR_TASKSHUTDOWN',
        N'WAIT_XTP_HOST_WAIT',              N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
        N'WAIT_XTP_CKPT_CLOSE',             N'XE_DISPATCHER_JOIN',
        N'XE_DISPATCHER_WAIT',              N'XE_TIMER_EVENT',
        N'DIRTY_PAGE_POLL',                 N'HADR_FILESTREAM_IOMGR_IOCOMPLETION')
                                """,
                                    'key_col': 'wait_type'
                                    },

               "query_stats": {'sql_text': """
                        -- PerfCollector
                        SELECT
                           raw_sql AS statement_text,
                           SUM(execution_count) AS execution_count ,
                           SUM(total_worker_time / 1000000) as total_worker_seconds,
                           SUM(min_worker_time / 1000000) as min_worker_seconds,
                           SUM(max_worker_time / 1000000) as max_worker_seconds,
                           SUM(total_physical_reads) AS total_physical_reads ,
                           SUM(min_physical_reads)  AS min_physical_reads,
                           SUM(max_physical_reads) AS max_physical_reads ,
                           SUM(total_logical_writes) AS total_logical_writes,
                           SUM(min_logical_writes) AS min_logical_writes,
                           SUM(max_logical_writes) AS max_logical_writes,
                           SUM(total_logical_reads) AS total_logical_reads,
                           SUM(min_logical_reads) AS min_logical_reads,
                           SUM(max_logical_reads) AS max_logical_reads,
                           SUM(total_clr_time) AS total_clr_time,
                           SUM(min_clr_time) AS min_clr_time,
                           SUM(max_clr_time) AS max_clr_time,
                           SUM(total_elapsed_time / 1000000) as total_elapsed_seconds,
                           SUM(min_elapsed_time / 1000000) as min_elapsed_seconds,
                           SUM(max_elapsed_time / 1000000) as max_elapsed_seconds,
                           SUM(total_rows) AS total_rows ,
                           SUM(min_rows) AS min_rows,
                           SUM(max_rows) AS max_rows
                        FROM sys.dm_exec_query_stats
                        CROSS APPLY (SELECT text, '' FROM sys.dm_exec_sql_text(sql_handle)
                                    ) AS query(raw_sql, statement_text)
                        where raw_sql not like '%PerfCollector%'
                        GROUP BY query.raw_sql


                                         """,
                               'key_col': 'statement_text'
                               },

               "dm_io_virtual_file_stats": {'sql_text': """
    -- PerfCollector
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
               -- PerfCollector
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
                        -- PerfCollector
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
               -- PerfCollector
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
