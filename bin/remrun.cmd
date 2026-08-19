@echo off
set REMRUN_ROOT=%~dp0\..
set PYTHONPATH=%REMRUN_ROOT%\src;%PYTHONPATH%
python -m remrun.cli %*
