$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$env:REMRUN_ROOT = Resolve-Path (Join-Path $ScriptDir "..")
$env:PYTHONPATH = "$env:REMRUN_ROOT\src;$env:PYTHONPATH"
python -m remrun.cli @args
