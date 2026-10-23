# Asks GitHub to run the KIM and GFS relay workflows now. Windows Task Scheduler on an
# always-on PC runs this every 30 minutes, because GitHub's own `schedule:` trigger does
# not fire reliably (it may be delayed for hours or dropped). Runs queue in each
# workflow's concurrency group, so an extra request while one runs costs nothing.
# Uses the PC's logged-in GitHub CLI; the log keeps the last 500 lines.
$ErrorActionPreference = 'Continue'
$repo = 'illegal-username/kma-kim-relay'
$gh = 'C:\Program Files\GitHub CLI\gh.exe'
$log = Join-Path $PSScriptRoot 'trigger-relays.log'

$lines = foreach ($workflow in 'relay.yml', 'gfs-relay.yml') {
    $output = & $gh workflow run $workflow -R $repo 2>&1 | Out-String
    $status = if ($LASTEXITCODE -eq 0) { 'ok' } else { 'FAILED: ' + $output.Trim() }
    '{0:yyyy-MM-dd HH:mm:ss} {1} {2}' -f (Get-Date), $workflow, $status
}
$previous = if (Test-Path $log) { Get-Content $log -Tail 498 } else { @() }
Set-Content -Path $log -Value (@($previous) + $lines) -Encoding utf8
