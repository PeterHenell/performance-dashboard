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
                         SELECT --sql_handle,
                               statement_start_offset ,
                               statement_end_offset ,
                               execution_count ,
                               total_worker_time ,
                               last_worker_time ,
                               min_worker_time ,
                               max_worker_time ,
                               total_physical_reads ,
                               last_physical_reads ,
                               min_physical_reads ,
                               max_physical_reads ,
                               total_logical_writes ,
                               last_logical_writes ,
                               min_logical_writes ,
                               max_logical_writes ,
                               total_logical_reads ,
                               last_logical_reads ,
                               min_logical_reads ,
                               max_logical_reads ,
                               total_clr_time ,
                               last_clr_time ,
                               min_clr_time ,
                               max_clr_time ,
                               total_elapsed_time ,
                               last_elapsed_time ,
                               min_elapsed_time ,
                               max_elapsed_time ,
                               total_rows ,
                               last_rows ,
                               min_rows ,
                               max_rows,
                               raw_sql,
                               statement_text
                         FROM sys.dm_exec_query_stats
                         CROSS APPLY (SELECT text,
                                             SUBSTRING(text, statement_start_offset / 2 + 1,
                                                            (CASE WHEN statement_end_offset = -1
                                                                    THEN LEN(CONVERT(nvarchar(MAX),text)) * 2
                                                                    ELSE statement_end_offset
                                                                END - statement_start_offset) / 2)
                                      FROM sys.dm_exec_sql_text(sql_handle)

                                     ) AS query(raw_sql, statement_text)""",
                   "procedure_stats": """
                             SELECT database_id ,
                                   object_id ,
                                   type ,
                                   type_desc ,
                                 --  sql_handle ,
                                --   plan_handle ,
                                   cached_time ,
                                   last_execution_time ,
                                   execution_count ,
                                   total_worker_time ,
                                   last_worker_time ,
                                   min_worker_time ,
                                   max_worker_time ,
                                   total_physical_reads ,
                                   last_physical_reads ,
                                   min_physical_reads ,
                                   max_physical_reads ,
                                   total_logical_writes ,
                                   last_logical_writes ,
                                   min_logical_writes ,
                                   max_logical_writes ,
                                   total_logical_reads ,
                                   last_logical_reads ,
                                   min_logical_reads ,
                                   max_logical_reads ,
                                   total_elapsed_time ,
                                   last_elapsed_time ,
                                   min_elapsed_time ,
                                   max_elapsed_time
                            FROM sys.dm_exec_procedure_stats"""
                   }
        return queries
