# def transform_dm_os_wait_stats(recs):
#     return [{r['wait_type'] + '_' + 'waiting_tasks_count': r['waiting_tasks_count'],
#              r['wait_type'] + '_' + 'wait_time_ms': r['wait_time_ms'],
#              r['wait_type'] + '_' + 'max_wait_time_ms': r['max_wait_time_ms'],
#              r['wait_type'] + '_' + 'signal_wait_time_ms': r['signal_wait_time_ms']
#              } for r in recs]
#
#
# def transform_query_stats(recs):
#     return [{r['wait_type'] + '_' + 'waiting_tasks_count': r['waiting_tasks_count'],
#              r['wait_type'] + '_' + 'wait_time_ms': r['wait_time_ms'],
#              r['wait_type'] + '_' + 'max_wait_time_ms': r['max_wait_time_ms'],
#              r['wait_type'] + '_' + 'signal_wait_time_ms': r['signal_wait_time_ms']
#              } for r in recs]
#
#
# def transform_dm_io_virtual_file_stats(recs):
#     return [{r['file_id'] + '_' + 'num_of_reads': r['num_of_reads'],
#              r['file_id'] + '_' + 'num_of_bytes_read': r['num_of_bytes_read'],
#              r['file_id'] + '_' + 'io_stall_read_ms': r['io_stall_read_ms'],
#              r['file_id'] + '_' + 'io_stall_queued_read_ms': r['io_stall_queued_read_ms'],
#              r['file_id'] + '_' + 'num_of_writes': r['num_of_writes'],
#              r['file_id'] + '_' + 'num_of_bytes_written': r['num_of_bytes_written'],
#              r['file_id'] + '_' + 'io_stall_write_ms': r['io_stall_write_ms'],
#              r['file_id'] + '_' + 'io_stall_queued_write_ms': r['io_stall_queued_write_ms'],
#              r['file_id'] + '_' + 'io_stall': r['io_stall'],
#              r['file_id'] + '_' + 'size_on_disk_bytes': r['size_on_disk_bytes']
#              } for r in recs]


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
                                            }
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

    # @staticmethod
    # def transform_results_for(recs, query_name):
    #     return QueryStore.queries[query_name]['transformer'](recs)
