<#
.SYNOPSIS
    Detached rebuild/restart helper for propbotd (docs/ARCHITECTURE.md Section 28.5).

.DESCRIPTION
    Spawned DETACHED by propbotd's /restart and /console commands. It waits two
    seconds for the daemon to finish replying, stops the running daemon (so
    target/release/propbotd.exe is unlocked), optionally runs the test suite and
    a release build, then relaunches the binary.

    Build failures are reported to the devops admin chat through the Telegram Bot
    API and the daemon is left stopped (no restart). The helper is idempotent and
    never assumes a particular working directory.

    NOTE: the daemon passes the old process id as -Pid. PowerShell reserves $PID
    (read-only), so the parameter is declared as $DaemonPid with an alias; callers
    still use -Pid.

.PARAMETER SkipBuild
    Skip `cargo test` + `cargo build --release` (used by /restart).
.PARAMETER BotToken
    Telegram bot token used to report failures.
.PARAMETER AdminChatId
    Telegram chat id that receives the failure/success notices.
.PARAMETER Pid
    Process id of the daemon to stop before relaunching.
.PARAMETER RepoRoot
    Repository root; defaults to the current directory.
.PARAMETER Binary
    Binary to launch; defaults to <RepoRoot>\target\release\propbotd.exe.
#>
param(
    [switch]$SkipBuild,
    [string]$BotToken,
    [string]$AdminChatId,
    [Alias('Pid')][int]$DaemonPid,
    [string]$RepoRoot,
    [string]$Binary
)

$ErrorActionPreference = 'Stop'

function Send-TelegramMessage {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($BotToken) -or [string]::IsNullOrWhiteSpace($AdminChatId)) {
        return
    }
    try {
        $uri = "https://api.telegram.org/bot$BotToken/sendMessage"
        $body = @{ chat_id = $AdminChatId; text = $Text }
        Invoke-RestMethod -Uri $uri -Method Post -Body $body -TimeoutSec 20 | Out-Null
    } catch {
        Write-Warning "telegram notify failed: $($_.Exception.Message)"
    }
}

try {
    # Let the daemon deliver the command reply before it is stopped.
    Start-Sleep -Seconds 2

    if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
        $RepoRoot = (Get-Location).Path
    }
    if ([string]::IsNullOrWhiteSpace($Binary)) {
        $Binary = Join-Path $RepoRoot 'target\release\propbotd.exe'
    }

    # Stop the daemon BEFORE building: on Windows a running executable locks
    # target/release/propbotd.exe, so `cargo build --release` cannot overwrite it.
    # Belt and suspenders: the pid we were given, plus any stray binary.
    if ($DaemonPid -gt 0) {
        Stop-Process -Id $DaemonPid -Force -ErrorAction SilentlyContinue
    }
    Stop-Process -Name propbotd -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1

    if (-not $SkipBuild) {
        Set-Location -LiteralPath $RepoRoot

        & cargo test
        if ($LASTEXITCODE -ne 0) {
            Send-TelegramMessage "propbotd: cargo test FAILED - not restarting."
            exit 1
        }

        & cargo build --release
        if ($LASTEXITCODE -ne 0) {
            Send-TelegramMessage "propbotd: cargo build --release FAILED - not restarting."
            exit 1
        }
    }

    if (-not (Test-Path -LiteralPath $Binary)) {
        Send-TelegramMessage "propbotd: binary not found at $Binary - not restarting."
        exit 1
    }

    Start-Process -FilePath $Binary -WorkingDirectory $RepoRoot
    Send-TelegramMessage "propbotd restarted."
} catch {
    Send-TelegramMessage "propbotd rebuild helper crashed: $($_.Exception.Message)"
    exit 1
}
