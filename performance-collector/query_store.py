class QueryStore:

    @staticmethod
    def get_query_text(query_name):
        return QueryStore.get_queries()[query_name]

    @staticmethod
    def get_query_names(self):
        return self.get_queries().keys()

    @staticmethod
    def get_queries():
        queries = {"dm_os_wait_stats": """
                            SELECT wait_type ,
                                   waiting_tasks_count ,
                                   wait_time_ms ,
                                   max_wait_time_ms ,
                                   signal_wait_time_ms
                            FROM sys.dm_os_wait_stats""",


                   "query_stats": """
SELECT statement_text,
   execution_count ,
   total_worker_time ,
   min_worker_time ,
   max_worker_time ,
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
   total_elapsed_time ,
   min_elapsed_time ,
   max_elapsed_time ,
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


                   "dm_io_virtual_file_stats": """
SELECT db_name(database_id) + '(' + CAST(file_id AS VARCHAR(10)) + ')' as dbname,
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
"""
                   }
        return queries

    @staticmethod
    def transform_results_for(recs, query_name):
        return [{r['wait_type'] + '_' + 'waiting_tasks_count': r['waiting_tasks_count'],
          r['wait_type'] + '_' + 'wait_time_ms': r['wait_time_ms'],
          r['wait_type'] + '_' + 'max_wait_time_ms': r['max_wait_time_ms'],
          r['wait_type'] + '_' + 'signal_wait_time_ms': r['signal_wait_time_ms']
          } for r in recs]
