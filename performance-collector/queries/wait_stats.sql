IF OBJECT_ID('tempdb..#prior_waits') IS NULL
BEGIN
	SELECT
		*
	INTO #prior_waits
	FROM sys.dm_os_wait_stats AS w
	
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
		*
	FROM sys.dm_os_wait_stats AS w
	
  SELECT
    *
  FROM
  (
	  SELECT
		  CASE n.m
			  WHEN 1 THEN 
				  p.wait_type + ' (wait time)'
			  WHEN 2 THEN
				  p.wait_type + ' (wait count)'
			  WHEN 3 THEN
				  p.wait_type + ' (maximum)'
			  ELSE 
				  'SIGNAL_WAIT_TIME'
		  END AS wait_type,
		  CASE n.m
			  WHEN 1 THEN
				  (c.wait_time_ms - c.signal_wait_time_ms) - (p.wait_time_ms - p.signal_wait_time_ms) 
			  WHEN 2 THEN
				  c.waiting_tasks_count - p.waiting_tasks_count
			  WHEN 3 THEN
				  p.max_wait_time_ms
			  ELSE
				  SUM(CASE WHEN n.m = 1 AND c.wait_type <> 'SLEEP_TASK' THEN c.signal_wait_time_ms ELSE 0 END) OVER () - 
					  SUM(CASE WHEN n.m = 1 AND p.wait_type <> 'SLEEP_TASK' THEN p.signal_wait_time_ms ELSE 0 END) OVER ()
		  END AS wait_delta
	  FROM #prior_waits AS p
	  INNER JOIN #current_waits AS c ON
		  c.wait_type = p.wait_type
		  AND c.signal_wait_time_ms >= p.signal_wait_time_ms
	  CROSS APPLY
	  (
		  SELECT 1
		  UNION ALL
		  SELECT 2
		  UNION ALL
		  SELECT 3
		  WHERE
			  p.max_wait_time_ms > c.max_wait_time_ms
		  UNION ALL
		  SELECT 4
		  WHERE
			  p.wait_type = 'SOS_SCHEDULER_YIELD'
	  ) AS n (m)
	  WHERE
		  p.wait_type NOT IN ('MISCELLANEOUS', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT')
  ) AS x
  WHERE
    x.wait_delta >= 0
	
	TRUNCATE TABLE #prior_waits
	
	INSERT #prior_waits WITH (TABLOCK)
	SELECT
		*
	FROM #current_waits
END