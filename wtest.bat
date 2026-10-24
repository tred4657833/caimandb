@echo off
setlocal

set "EXE=caimandb.exe"
set "SCRIPT=insert_test.nql"

if not exist "%EXE%" (
    echo ERROR: No se encontro %EXE%
    pause
    exit /b 1
)

(
echo CREATE DB benchdb
echo USE benchdb
echo CREATE BLOCK users

for /L %%i in (1,1,100) do (
    echo INSERT users {"id":%%i,"name":"Usuario%%i","email":"usuario%%i@test.com","age":25}
)

echo COUNT users
echo EXIT
) > "%SCRIPT%"

"%EXE%" RUN "%SCRIPT%"

pause
