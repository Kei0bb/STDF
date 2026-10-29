@echo off
rem Repair %USERPROFILE%\.marimo after a failed notebook save (Permission denied on the lock file).
rem Python 3.13 on Windows turns mkdir mode=0o700 into an ACL that can lock the owner out of
rem notebook-locks, even for delete. The parent .marimo stays ours, so this RENAMES it out of
rem the way and recreates the lock folder WITHOUT a mode (marimo then reuses it).
rem Stop marimo first (Ctrl+C in its window). Keep this file ASCII and CRLF.
setlocal
set "D=%USERPROFILE%\.marimo"
echo [fix] target: %D%
if not exist "%D%" goto make
echo [fix] trying to delete the old folder
icacls "%D%" /reset /t /c >/dev/null 2>nul
rmdir /s /q "%D%" 2>nul
if not exist "%D%" goto make
echo [fix] cannot delete it, renaming it out of the way instead
ren "%D%" ".marimo_broken_%RANDOM%"
if exist "%D%" goto failed
:make
echo [fix] creating the lock folder
mkdir "%D%\notebook-locks"
if errorlevel 1 goto failed
echo test> "%D%\notebook-locks\_fix_test.tmp"
if errorlevel 1 goto failed
del "%D%\notebook-locks\_fix_test.tmp"
echo [fix] OK. Start scripts\start_marimo.bat and save a notebook.
echo [fix] A folder named .marimo_broken_* may remain in %USERPROFILE%; an administrator can delete it later.
pause
exit /b 0
:failed
echo [fix] FAILED. Send the output below.
icacls "%D%"
pause
exit /b 1
