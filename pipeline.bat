@echo off
set "DIR=%~dp0"
set "PYTHONPATH=%DIR%src;%DIR%;%PYTHONPATH%"
python -m heartbeat.cli %*
