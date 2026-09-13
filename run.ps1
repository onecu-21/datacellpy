# Keep Python flags such as -v out of PowerShell's common-parameter binding.
$PythonArguments = @($args)
$ErrorActionPreference = 'Stop'
if (-not $PythonArguments) {
    Write-Output 'Usage: .\run.ps1 <Python arguments>'
    Write-Output 'Run tests: .\run.ps1 -m unittest discover -s tests -v'
    exit 0
}
$bundledPython = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$localPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
$candidates = @($localPython)
$installedPython = Get-Command python -ErrorAction SilentlyContinue
if ($installedPython) { $candidates += $installedPython.Source }
$candidates += $bundledPython
$chosenPython = $null
foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate) {
        & $candidate -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>$null
        if ($LASTEXITCODE -eq 0) {
            $chosenPython = $candidate
            break
        }
    }
}
if (-not $chosenPython) { throw 'Python 3.10+ is required. Install Python or create .venv.' }

Push-Location -LiteralPath $PSScriptRoot
try {
    & $chosenPython -X utf8 @PythonArguments
    $runExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $runExitCode
