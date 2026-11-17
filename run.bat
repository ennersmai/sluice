@echo off
cd /d "%~dp0"
set "EXE="
if exist "sluice.exe" set "EXE=sluice.exe"
if not defined EXE if exist "sluice-windows-x64.exe" set "EXE=sluice-windows-x64.exe"
if not defined EXE if exist "target\release\sluice.exe" set "EXE=target\release\sluice.exe"
if not defined EXE (
  echo sluice.exe not found next to run.bat.
  pause
  exit /b 1
)
"%EXE%" %*
if errorlevel 1 (
  echo.
  echo sluice exited with an error. See the message above.
  pause
)
