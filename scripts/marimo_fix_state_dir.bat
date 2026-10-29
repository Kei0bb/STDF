@echo off
rem Repair %USERPROFILE%\.marimo after a failed notebook save (Permission denied on the lock file).
rem Python 3.13 on Windows turns mkdir mode=0o700 into an ACL that can lock the owner out.
rem This takes ownership, resets the ACL to inherited, deletes the folder and recreates it
rem WITHOUT a mode, so marimo reuses it. Stop marimo first (Ctrl+C in its window).
rem If a step is denied, run this from an Administrator prompt. Keep this file ASCII and CRLF.
setlocal
set "D=%USERPROFILE%\.marimo"
echo [fix] target: %D%
if not exist "%D%" goto make
echo [fix] taking ownership
takeown /f "%D%" /r /d y >nul
echo [fix] resetting permissions to inherited
icacls "%D%" /reset /t /c >nul
echo [fix] removing the old folder
rmdir /s /q "%D%"
if exist "%D%" goto failed
:make
echo [fix] creating the lock folder
mkdir "%D%\notebook-locks"
if errorlevel 1 goto failed
echo test> "%D%\notebook-locks\_fix_test.tmp"
if errorlevel 1 goto failed
del "%D%\notebook-locks\_fix_test.tmp"
echo [fix] OK. Start scripts\start_marimo.bat and save a notebook.
pause
exit /b 0
:failed
echo [fix] FAILED. Run this from an Administrator prompt, or send the output below.
icacls "%D%"
pause
exit /b 1
