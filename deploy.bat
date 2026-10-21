@echo off
REM Builds the images and starts all five containers.
docker compose up -d --build
echo.
echo Open http://localhost:8080 in your browser.
echo To crash the leader:    docker compose stop coordinator_2
echo To bring it back:       docker compose start coordinator_2
pause
