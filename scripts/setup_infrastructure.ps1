param(
    [string]$RepositoryUrl = "https://github.com/myamafuj/hadoop-hive-spark-docker.git",
    [string]$TargetDirectory = "infrastructure"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$targetPath = Join-Path $projectRoot $TargetDirectory
$overridePath = Join-Path $projectRoot "config\hadoop\mapred-site.xml"
$destinationPath = Join-Path $targetPath "base\conf\mapred-site.xml"

if (-not (Test-Path -LiteralPath $targetPath)) {
    git -c core.autocrlf=false clone $RepositoryUrl $targetPath
} elseif (-not (Test-Path -LiteralPath (Join-Path $targetPath ".git"))) {
    throw "Target exists but is not the expected Git clone: $targetPath"
}

Copy-Item -LiteralPath $overridePath -Destination $destinationPath -Force

Write-Host "Infrastructure is ready at $targetPath"
Write-Host "The tracked MapReduce/YARN override has been applied."
