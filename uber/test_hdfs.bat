@echo off
echo Testing Uber HDFS System...

javac UberHDFS.java

if %errorlevel% neq 0 (
    echo Compilation failed!
    pause
    exit /b 1
)

echo Running HDFS test...
java UberHDFS

pause