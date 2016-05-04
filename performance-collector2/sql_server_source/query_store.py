class SqlQuery:
    def __init__(self, query_text, query_name, key_column):
        self.query_text = query_text
        self.query_name = query_name
        self.key_column = key_column


class QueryStore:
    def __init__(self):
        self.queries = []

        for k, v in QueryStore.get_query_dict().items():
            query_name = k
            query_text = v['sql_text']
            key_column = v['key_col']
            q = SqlQuery(query_name=query_name, query_text=query_text, key_column=key_column)
            self.queries.append(q)

    @staticmethod
    def get_query_dict():
        return {"dm_os_wait_stats": {'sql_text': """
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
               """, 'key_col': 'session_id'},

                "dm_os_performance_counters": {'sql_text': """
               -- PerfCollector
                SELECT  'dm_os_performance_counters'  AS key_col
                    , [Page life expectancy] AS [Page_life_expectancy]
                    , CAST([Buffer cache hit ratio] AS DECIMAL(28, 6)) / CAST([Buffer cache hit ratio base] AS DECIMAL(28, 6)) AS [Cache_Hit_Ratio]
                    , CAST([Average Wait Time (ms)] AS DECIMAL(28, 6)) / CAST([Average Wait Time Base] AS DECIMAL(28, 6)) AS [Avarage_Lock_Wait_Time]
                    , CAST([CPU usage %] AS DECIMAL(28, 6)) / CAST([CPU usage % base] AS DECIMAL(28, 6)) AS [CPU_Usage_Pcnt]
                    , CAST([Avg Disk Read IO (ms)] AS DECIMAL(28, 6)) / CAST([Avg Disk Read IO (ms) base] AS DECIMAL(28, 6)) AS [Avg_Disk_Read_IO_ms]
                    , CAST([Avg Disk Write IO (ms)] AS DECIMAL(28, 6)) / CAST([Avg Disk Write IO (ms) Base] AS DECIMAL(28, 6)) AS [Avg_Disk_Write_IO_ms]
                    , [Page lookups/sec] AS [Page_lookups_per_sec]
                    , [Lazy writes/sec] AS [Lazy_writes_per_sec]
                    , [Readahead pages/sec] AS [Readahead_pages_per_sec]
                    , [Readahead time/sec] AS [Readahead_time_per_sec]
                    , [Page reads/sec] AS [Page_reads_per_sec]
                    , [Page writes/sec] AS [Page_writes_per_sec]
                    , [Free list stalls/sec] AS [Free_list_stalls_per_sec]
                    , 100 * (CAST([Free list stalls/sec] AS DECIMAL(28, 6)) / CAST([Page reads/sec]  AS DECIMAL(28, 6))) AS [Readahead_Pcnt_of_Reads_per_sec]
                    , 20.0 AS [Readahead_Pcnt_of_Reads_per_sec_Threshold]
                    , [Transactions/sec] AS [Transactions_per_sec]
                    , [Number of Deadlocks/sec] AS [Number_of_Deadlocks_per_sec]
                    , [SQL Compilations/sec] AS [SQL_Compilations_per_sec]
                    , [SQL Re-Compilations/sec] AS [SQL_ReCompilations_per_sec]
                    , [Batch Requests/sec] AS [Batch_Requests_per_sec]
                    , [Full Scans/sec] AS [Full_Scans_per_sec]
                    , [Page Splits/sec] AS [Page_Splits_per_sec]
                    FROM
                    (
                        SELECT cntr_value, RTRIM(counter_name) AS counter_name
                        FROM sys.dm_os_performance_counters
                        WHERE
                            (object_name = 'SQLServer:Locks' AND counter_name IN('Average Wait Time Base', 'Average Wait Time (ms)') AND instance_name = '_Total')
                            OR
                            (object_name = 'SQLServer:Workload Group Stats' AND counter_name IN('CPU usage %', 'CPU usage % base') AND instance_name = 'default')
                            OR
                            (object_name = 'SQLServer:Resource Pool Stats' AND counter_name IN('Avg Disk Read IO (ms)', 'Avg Disk Read IO (ms) Base') AND instance_name = 'default')
                             OR
                            (object_name = 'SQLServer:Resource Pool Stats' AND counter_name IN('Avg Disk Write IO (ms)', 'Avg Disk Write IO (ms) Base') AND instance_name = 'default')
                             OR
                            (object_name = 'SQLServer:Buffer Manager' AND counter_name IN('Buffer cache hit ratio', 'Buffer cache hit ratio base'))
                             OR
                            (object_name = 'SQLServer:Access Methods' AND counter_name IN('Full Scans/sec', 'Page Splits/sec'))
                            OR
                            (counter_name = 'Transactions/sec' AND instance_name = '_Total')
                            OR
                            (counter_name IN ('Page lookups/sec', 'Lazy writes/sec', 'Readahead pages/sec',
                                                'Readahead time/sec', 'Page reads/sec', 'Page writes/sec',
                                                'Free list stalls/sec',
                                                'Number of Deadlocks/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec',
                                                'Batch Requests/sec', 'Buffer cache hit ratio base', 'Page life expectancy')
                            )
                    ) src
                    PIVOT (
                        SUM(cntr_value)
                        FOR counter_name IN ( [Average Wait Time (ms)]
                                            , [Average Wait Time Base]
                                            , [Avg Disk Read IO (ms) Base]
                                            , [Avg Disk Read IO (ms)]
                                            , [Avg Disk Write IO (ms) Base]
                                            , [Avg Disk Write IO (ms)]
                                            , [Batch Requests/sec]
                                            , [Buffer cache hit ratio base]
                                            , [Buffer cache hit ratio]
                                            , [CPU usage % base]
                                            , [CPU usage %]
                                            , [Free list stalls/sec]
                                            , [Lazy writes/sec]
                                            , [Number of Deadlocks/sec]
                                            , [Page lookups/sec]
                                            , [Page reads/sec]
                                            , [Page writes/sec]
                                            , [Readahead pages/sec]
                                            , [Readahead time/sec]
                                            , [SQL Compilations/sec]
                                            , [SQL Re-Compilations/sec]
                                            , [Transactions/sec]
                                            , [Page life expectancy]
                                            , [Full Scans/sec]
                                            , [Page Splits/sec]
                                            )
                        ) AS pivoted
               """, 'key_col': 'key_col'}
                }

        # @staticmethod
        # def get_query_text(query_name):
        #     return QueryStore.queries[query_name]['sql_text']
        #
        # @staticmethod
        # def get_query_key_col(query_name):
        #     return QueryStore.queries[query_name]['key_col']
        #
        # @staticmethod
        # def get_query_names(self):
        #     return self.queries.keys()
