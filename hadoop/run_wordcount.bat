@echo off
REM Step by step Hadoop word count on the seat booking log.
REM Run this from the project root folder, with the seat booking system already running.

echo 1. Saving the booking activity log from the leader...
curl -s http://localhost:5000/api/export-log -o hadoop\job\input\booking_log.txt
if errorlevel 1 curl -s http://localhost:5010/api/export-log -o hadoop\job\input\booking_log.txt

echo 2. Starting the Hadoop cluster (first run downloads the image, be patient)...
docker compose -f hadoop\docker-compose.yml up -d
echo    Waiting 45 seconds for HDFS and YARN to be ready...
timeout /t 45 /nobreak > nul

echo 3. Running the word count...
docker compose -f hadoop\docker-compose.yml exec resourcemanager bash /opt/job/run_job.sh

echo.
echo HDFS page: http://localhost:9870    YARN page: http://localhost:8088
pause
