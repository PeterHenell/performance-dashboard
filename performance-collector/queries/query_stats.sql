IF OBJECT_ID('tempdb..#prior_times') IS NULL
BEGIN
	SELECT
		query_hash,
		SUM(execution_count) AS execution_count,
		SUM(total_worker_time) AS total_worker_time,
		SUM(total_physical_reads) AS total_physical_reads,
		SUM(total_logical_reads) AS total_logical_reads,
		SUM(total_logical_writes) AS total_logical_writes,
		SUM(total_clr_time) AS total_clr_time,
		SUM(total_elapsed_time) AS total_elapsed_time
	INTO #prior_times
	FROM sys.dm_exec_query_stats
	GROUP BY
		query_hash
	
	SELECT 
		*
	INTO #current_times
	FROM #prior_times
	WHERE
		1=0
END
ELSE
BEGIN	
	TRUNCATE TABLE #current_times

	INSERT #current_times WITH (TABLOCK)
	SELECT
		query_hash,
		SUM(execution_count) AS execution_count,
		SUM(total_worker_time) AS total_worker_time,
		SUM(total_physical_reads) AS total_physical_reads,
		SUM(total_logical_reads) AS total_logical_reads,
		SUM(total_logical_writes) AS total_logical_writes,
		SUM(total_clr_time) AS total_clr_time,
		SUM(total_elapsed_time) AS total_elapsed_time
	FROM sys.dm_exec_query_stats
	GROUP BY
		query_hash
	
	SELECT
		CONVERT(VARCHAR, c.query_hash, 1) + ' : ' +
			CASE x.n
				WHEN 1 THEN 'execution count'
				WHEN 2 THEN 'worker time (avg)'
				WHEN 3 THEN 'physical reads (avg)'
				WHEN 4 THEN 'logical reads (avg)'
				WHEN 5 THEN 'logical writes (avg)'
				WHEN 6 THEN 'CLR time (avg)'
				WHEN 7 THEN 'elapsed time (avg)'
			END AS metric,
		CASE x.n
			WHEN 1 THEN c.execution_count - p.execution_count
			WHEN 2 THEN (c.total_worker_time - p.total_worker_time) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
			WHEN 3 THEN (c.total_physical_reads - p.total_physical_reads) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
			WHEN 4 THEN (c.total_logical_reads - p.total_logical_reads) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
			WHEN 5 THEN (c.total_logical_writes - p.total_logical_writes) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
			WHEN 6 THEN (c.total_clr_time - p.total_clr_time) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
			WHEN 7 THEN (c.total_elapsed_time - p.total_elapsed_time) / ISNULL(NULLIF((c.execution_count - p.execution_count), 0), 1)
		END AS metric_value
	FROM #prior_times AS p
	INNER JOIN #current_times AS c ON
		c.query_hash = p.query_hash
		AND c.execution_count > p.execution_count
	CROSS APPLY
	(
		SELECT 1
		UNION ALL
		SELECT 2
		UNION ALL
		SELECT 3
		UNION ALL
		SELECT 4
		UNION ALL
		SELECT 5
		UNION ALL
		SELECT 6
		UNION ALL
		SELECT 7
	) AS x (n)
	
	TRUNCATE TABLE #prior_times
	
	INSERT #prior_times WITH (TABLOCK)
	SELECT
		*
	FROM #current_times

	--populate info into the "dba" database
	--change this if you don't want this script automatically creating databases, etc
	IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'dba')
	BEGIN
		EXEC('CREATE DATABASE dba')
	END

	IF OBJECT_ID('dba.dbo.query_info') IS NULL
	BEGIN
		EXEC
		('
			CREATE TABLE dba.dbo.query_info
			(
				query_hash BINARY(8) NOT NULL,
				query_plan_hash BINARY(8) NOT NULL,
				query_text NVARCHAR(MAX),
				query_plan NVARCHAR(MAX),
				PRIMARY KEY (query_hash, query_plan_hash)
			)
		')
	END
	
	INSERT dba.dbo.query_info
	(
		query_hash,
		query_plan_hash,
		query_text,
		query_plan		
	)
	SELECT
		x.query_hash,
		x.query_plan_hash,	
		(
			SELECT
				SUBSTRING
				(
					est.text,
					((x.statement_start_offset/2) + 1),
					(
						CASE
							WHEN x.statement_end_offset = -1 THEN 2147483647
							ELSE ((x.statement_end_offset - x.statement_start_offset)/2) + 1
						END
					)
				) AS query_text
			FROM sys.dm_exec_sql_text(x.sql_handle) AS est
		) AS query_text,
		(
			SELECT
				etq.query_plan
			FROM sys.dm_exec_text_query_plan(x.plan_handle, x.statement_start_offset, x.statement_end_offset) AS etq
		) AS query_plan
	FROM #current_times AS c
	CROSS APPLY
	(
		SELECT
			qs.query_hash,
			qs.query_plan_hash,
			qs.sql_handle,
			qs.plan_handle,
			qs.statement_start_offset,
			qs.statement_end_offset,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					qs.query_hash,
					qs.query_plan_hash
				ORDER BY
					(SELECT NULL)
			) AS r
		FROM sys.dm_exec_query_stats AS qs
		WHERE
			c.query_hash = qs.query_hash
			AND NOT EXISTS
			(
				SELECT *
				FROM dba.dbo.query_info AS qi			
				WHERE
					qi.query_hash = qs.query_hash
					AND qi.query_plan_hash = qs.query_plan_hash
			)
	) AS x
	WHERE
		x.r = 1
END