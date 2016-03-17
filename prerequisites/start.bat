@echo off

REM Starts both es and kibana in separate cmd sessions
start "Elasticsearch" "C:\src\github\performance-dashboard\prerequisites\elasticsearch-2.2.0\bin\elasticsearch.bat"
start "Kibana" "C:\src\github\performance-dashboard\prerequisites\kibana-4.4.1-windows\kibana-4.4.1-windows\bin\kibana.bat"