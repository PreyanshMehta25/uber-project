@echo off
echo Fixing Unicode characters in Java files...

powershell -Command "(Get-Content 'UberServer.java' -Raw) -replace , 'HEARTBEAT' -replace , 'ALERT' -replace '⚠️', 'WARNING' -replace '✅', 'SUCCESS' -replace '❌', 'ERROR' -replace '🔄', 'RECOVERY' -replace '📦', 'DATA-MIGRATION' -replace '🌐', 'NETWORK' -replace '🏥', 'HEALTH' -replace '📊', 'PERFORMANCE' -replace '💾', 'BACKUP' -replace '🔍', 'INTEGRITY' -replace '🚗', 'RIDES' -replace '👨‍💼', 'DRIVERS' -replace '📸', 'SNAPSHOT' -replace '🔧', 'REPAIR' -replace '📝', 'LOG' -replace '🗳️', 'ELECTION' -replace '🧪', 'TEST' -replace '📡', 'DISCONNECT' -replace '🔗', 'RECONNECT' | Set-Content 'UberServer.java'"

powershell -Command "(Get-Content 'UberGUI.java' -Raw) -replace , '*' -replace , 'WARNING' | Set-Content 'UberGUI.java'"

echo Unicode characters fixed!