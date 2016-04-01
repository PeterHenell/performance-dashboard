# performance-dashboard
Dashboard for performance statistics currently specific to collect data from from SQL Server and MS Windows but with the intentiontion to be used for any source.

## What does it do?
* It collects performance measurements from the server (logical bytes read, query execution time etc) every 15 minutes. It calculates the difference between the previous measurement and pushes this delta to Elasticsearch with a timestamp.
* Currently only sql queries are supported.

## TODO:
* See https://github.com/PeterHenell/performance-dashboard/issues

## Example:
![example dashboard](https://github.com/PeterHenell/performance-dashboard/blob/master/dashboard.png)
