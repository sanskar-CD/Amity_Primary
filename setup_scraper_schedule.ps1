#Requires -Version 5.1
<#
.SYNOPSIS
  Registers a Windows Scheduled Task to run cron_scrape.py on a fixed interval (current user, when logged on).

.PARAMETER IntervalMinutes
  How often to run. Default 30 (every 30 minutes).

.EXAMPLE
  .\setup_scraper_schedule.ps1
  # Every 30 minutes (default)

.EXAMPLE
  .\setup_scraper_schedule.ps1 -IntervalMinutes 30
  # Every 30 minutes
#>
param(
    [Parameter(Mandatory = $false)]
    [ValidateRange(1, 1439)]
    [int]$IntervalMinutes = 30
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$TaskName = "AmityPrimary-PortalScrape-$($IntervalMinutes)m"

$scriptPath = Join-Path $Root "cron_scrape.py"
$exe = $null
$argString = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    $exe = (Get-Command py).Source
    $argString = "-3 `"$scriptPath`""
}
elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $exe = (Get-Command python).Source
    $argString = "`"$scriptPath`""
}
else {
    throw "Neither 'py' nor 'python' was found in PATH. Install Python 3 and retry."
}

$Action = New-ScheduledTaskAction -Execute $exe -Argument $argString -WorkingDirectory $Root
$Start = (Get-Date).AddMinutes(1)
$Trigger = New-ScheduledTaskTrigger -Once -At $Start `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

# Remove any previous versions (including old hardcoded name).
Unregister-ScheduledTask -TaskName "AmityPrimary-PortalScrape-30m" -Confirm:$false -ErrorAction SilentlyContinue
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Force | Out-Null

Write-Host "OK: Scheduled task '$TaskName' runs every $IntervalMinutes minute(s) (working dir: $Root)."
Write-Host "    Python: $exe $argString"
Write-Host "    First run window starts at $Start, then every $IntervalMinutes min."
