@echo off
cd /d "%~dp0"
sluice.exe %*
if errorlevel 1 (
  echo.
  echo sluice exited with an error. See the message above.
  pause
)
