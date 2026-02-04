@echo off
setlocal


:: relaunch in Windows Terminal if available
if not defined WT_SESSION if not defined WT_RELAUNCHED (
  where wt >nul 2>&1
  if %errorlevel%==0 (
    wt -d "%~dp0" cmd /c "set WT_RELAUNCHED=1 & \"%~f0\" %*"
    exit /b
  )
)

cd grinder
fnm use 24 2>nul
set "LOG_TEE_FILE=logs\summarize.log"
call npm run summarize
set "LOG_TEE_FILE="
pause