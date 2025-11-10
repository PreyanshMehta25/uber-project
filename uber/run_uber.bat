@echo off
echo Starting Uber System...

javac UberGUI.java UberServer.java

if %errorlevel% neq 0 (
    echo Compilation failed!
    pause
    exit /b 1
)

echo Starting server...
start "Uber Server" cmd /k "java UberServer"

timeout /t 2 /nobreak >nul

echo Starting GUI...
java UberGUI

pause