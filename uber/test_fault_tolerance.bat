@echo off
echo ========================================
echo    UBER FAULT TOLERANCE TEST SCRIPT
echo ========================================
echo.
echo This script will test various fault tolerance features
echo Make sure UberServer is running before proceeding
echo.
pause

echo Testing Health Status...
echo HEALTH_STATUS | telnet localhost 8080
timeout /t 2 > nul

echo.
echo Testing Backup Status...
echo BACKUP_STATUS | telnet localhost 8080
timeout /t 2 > nul

echo.
echo Simulating Node 3 Failure...
echo SIMULATE_FAILURE;3 | telnet localhost 8080
timeout /t 3 > nul

echo.
echo Checking Health After Failure...
echo HEALTH_STATUS | telnet localhost 8080
timeout /t 2 > nul

echo.
echo Simulating Network Partition...
echo SIMULATE_PARTITION | telnet localhost 8080
timeout /t 3 > nul

echo.
echo Checking Health After Partition...
echo HEALTH_STATUS | telnet localhost 8080
timeout /t 2 > nul

echo.
echo Recovering from Partition...
echo RECOVER_PARTITION | telnet localhost 8080
timeout /t 3 > nul

echo.
echo Final Health Check...
echo HEALTH_STATUS | telnet localhost 8080

echo.
echo ========================================
echo    FAULT TOLERANCE TEST COMPLETED
echo ========================================
echo Check the UberServer terminal for detailed fault tolerance logs
pause