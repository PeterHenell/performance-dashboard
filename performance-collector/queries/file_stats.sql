IF OBJECT_ID('tempdb..#prior_waits') IS NULL
BEGIN
	SELECT
		d.name,
		w.*
	INTO #prior_waits
	FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS w
	INNER JOIN sys.databases AS d ON
		d.database_id = w.database_id
	
	SELECT 
		*
	INTO #current_waits
	FROM #prior_waits
	WHERE
		1=0
END
ELSE
BEGIN	
	TRUNCATE TABLE #current_waits

	INSERT #current_waits WITH (TABLOCK)
	SELECT
		d.name,
		w.*
	FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS w
	INNER JOIN sys.databases AS d ON
		d.database_id = w.database_id
	
	SELECT
		c.name + '(' + CONVERT(VARCHAR, c.file_id) + '):' +
			CASE x.n
				WHEN 1 THEN 'num_of_reads'
				WHEN 2 THEN 'num_of_bytes_read'
				WHEN 3 THEN 'io_stall_read_ms'
				WHEN 4 THEN 'num_of_writes'
				WHEN 5 THEN 'num_of_bytes_written'
				WHEN 6 THEN 'io_stall_write_ms'
				WHEN 7 THEN 'io_stall'
				WHEN 8 THEN 'size_on_disk_bytes (total)'
				WHEN 9 THEN 'size_on_disk_bytes (delta)'
			END AS metric,
		CASE x.n
			WHEN 1 THEN c.num_of_reads - p.num_of_reads
			WHEN 2 THEN c.num_of_bytes_read - p.num_of_bytes_read
			WHEN 3 THEN c.io_stall_read_ms - p.io_stall_read_ms
			WHEN 4 THEN c.num_of_writes - p.num_of_writes
			WHEN 5 THEN c.num_of_bytes_written - p.num_of_bytes_written
			WHEN 6 THEN c.io_stall_write_ms - p.io_stall_write_ms
			WHEN 7 THEN c.io_stall - p.io_stall
			WHEN 8 THEN c.size_on_disk_bytes
			WHEN 9 THEN c.size_on_disk_bytes - p.size_on_disk_bytes
		END AS metric_value
	FROM #prior_waits AS p
	INNER JOIN #current_waits AS c ON
		c.name = p.name
		AND c.file_id = p.file_id
		AND c.num_of_bytes_read >= p.num_of_bytes_read
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
		UNION ALL
		SELECT 8
		UNION ALL
		SELECT 9
	) AS x (n)
	
	TRUNCATE TABLE #prior_waits
	
	INSERT #prior_waits WITH (TABLOCK)
	SELECT
		*
	FROM #current_waits
END