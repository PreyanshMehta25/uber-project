@echo off
echo ========================================
echo    UBER FAULT TOLERANCE DEMO
echo ========================================
echo.
echo Starting Uber Server with Fault Tolerance...
echo.
echo FAULT TOLERANCE FEATURES:
echo - Node Failure Detection and Recovery
echo - Service Health Monitoring  
echo - Data Backup and Integrity Checks
echo - Network Partition Handling
echo - Automatic Leader Election
echo.
echo Watch the terminal for detailed fault tolerance logs!
echo.
echo To test fault tolerance:
echo 1. Use the GUI "Fault Tolerance" menu
echo 2. Or connect via telnet: telnet localhost 8080
echo 3. Commands: HEALTH_STATUS, SIMULATE_FAILURE;3, etc.
echo.
pause

java UberServer_Clean