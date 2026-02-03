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

::git pull

cd grinder
FOR /f "tokens=*" %%i IN ('fnm env --use-on-cd') DO CALL %%i
fnm use 24 2>nul
call npm i --loglevel=error

call npm run cleanup auto > logs/cleanup.log
del ..\audio\*.mp3 >nul 2>&1
del ..\img\*.jpg >nul 2>&1
del ..\img\screenshots.txt >nul 2>&1
del articles\*.txt >nul 2>&1
del articles\*.html >nul 2>&1


::call npm run load auto > logs/load.log
set "LOG_TEE_FILE=logs\summarize.log"
call npm run summarize auto
set "LOG_TEE_FILE="
call npm run slides auto > logs/slides.log

call npm run screenshots > logs/screenshots.log
call npm run upload-img > logs/upload-img.log
call npm run audio auto > logs/audio.log
