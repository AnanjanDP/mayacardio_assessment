# Never Lose a Heartbeat CLI wrapper for PowerShell
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:PYTHONPATH = "$scriptDir\src;$scriptDir;$env:PYTHONPATH"
python -m heartbeat.cli @args
