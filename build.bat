@echo off
setlocal

title Building CaimanDB

cd /d "%~dp0"

echo ======================================
echo        Building CaimanDB
echo ======================================
echo.

echo [1/6] Resolving newer engine dependencies (Ristretto, Roaring, msgpack, etc.)...
REM These were hand-added to go.mod without network access, so go.sum has no
REM entries for them yet (the "missing go.sum entry" error) and their pinned
REM versions were never verified against the module proxy. `go get
REM ^<module^>@latest` re-resolves each one for real and rewrites go.mod/go.sum
REM with a version that is guaranteed to exist. Safe to re-run.
go get github.com/dgraph-io/ristretto/v2@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/dgraph-io/ristretto/v2.
    pause
    exit /b 1
)
go get github.com/RoaringBitmap/roaring@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/RoaringBitmap/roaring.
    pause
    exit /b 1
)
go get golang.org/x/sync@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve golang.org/x/sync.
    pause
    exit /b 1
)
go get github.com/vmihailenco/msgpack/v5@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/vmihailenco/msgpack/v5.
    pause
    exit /b 1
)
go get github.com/google/uuid@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/google/uuid.
    pause
    exit /b 1
)
go get github.com/natefinch/atomic@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/natefinch/atomic.
    pause
    exit /b 1
)
go get github.com/smallnest/ringbuffer@latest
if errorlevel 1 (
    echo.
    echo Failed to resolve github.com/smallnest/ringbuffer.
    pause
    exit /b 1
)

echo.
echo [2/6] Running go mod tidy...
go mod tidy
if errorlevel 1 (
    echo.
    echo Failed to run go mod tidy.
    pause
    exit /b 1
)

echo.
echo [3/6] Downloading dependencies...
go mod download
if errorlevel 1 (
    echo.
    echo Failed to download dependencies.
    pause
    exit /b 1
)

echo.
echo [4/6] Building cli.exe...
go build -o cli.exe ./cli/main.go
if errorlevel 1 (
    echo.
    echo Failed to build cli.exe.
    pause
    exit /b 1
)

echo.
echo [5/6] Building CaimanDB...
go build ./cmd/caimandb
if errorlevel 1 (
    echo.
    echo Failed to build CaimanDB.
    pause
    exit /b 1
)

echo.
echo ======================================
echo      Build completed successfully
echo ======================================

pause