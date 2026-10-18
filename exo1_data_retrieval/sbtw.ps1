# Lanceur Windows local : aucun logiciel ni paramètre système n'est installé.
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$SbtCommands = @('test'))

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding
$sbtVersion = '1.10.11'
$launcherHash = 'E988D533A020E5B60EC22C3B5DF4CD3E3DF465F4FDC3951C63036E21483978E4'

$javaCommand = Get-Command java -ErrorAction SilentlyContinue
if ($env:JAVA_HOME) {
    $javaPath = Join-Path $env:JAVA_HOME 'bin/java.exe'
} elseif ($javaCommand) {
    $javaPath = $javaCommand.Source
} else {
    # Sur ce PC, IntelliJ contient déjà un runtime Java utilisable par SBT.
    $jetbrains = Join-Path $env:ProgramFiles 'JetBrains'
    $javaPath = Get-ChildItem -LiteralPath $jetbrains -Directory -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending |
        ForEach-Object { Join-Path $_.FullName 'jbr/bin/java.exe' } |
        Where-Object { Test-Path -LiteralPath $_ } |
        Select-Object -First 1
}
if (!$javaPath -or !(Test-Path -LiteralPath $javaPath)) {
    throw 'Java introuvable. Installer un JDK 21 ou définir JAVA_HOME vers un JDK existant.'
}

Push-Location $PSScriptRoot
$previousCache = $env:COURSIER_CACHE
try {
    New-Item -ItemType Directory -Force -Path '.tools' | Out-Null
    $launcher = Join-Path $PSScriptRoot '.tools/sbt-launch.jar'
    if (!(Test-Path -LiteralPath $launcher)) {
        # TLS 1.2 permet aussi l'utilisation depuis Windows PowerShell 5.1.
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 120 -Uri (
            "https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/$sbtVersion/sbt-launch-$sbtVersion.jar"
        ) -OutFile $launcher
    }
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $launcher).Hash -ne $launcherHash) {
        throw 'Empreinte du lanceur SBT incorrecte. Vérifier .tools/sbt-launch.jar avant de réessayer.'
    }
    $env:COURSIER_CACHE = Join-Path $PSScriptRoot '.tools/coursier-cache'
    $options = @(
        '-Dfile.encoding=UTF-8',
        '-Dstdout.encoding=UTF-8',
        '-Dstderr.encoding=UTF-8',
        '-Dsbt.global.base=.tools/sbt-global',
        '-Dsbt.boot.directory=.tools/sbt-boot',
        '-Dsbt.ivy.home=.tools/ivy',
        '-Dsbt.coursier.home=.tools/coursier',
        '-Dsbt.override.build.repos=true',
        '-Dsbt.repository.config=project/repositories'
    )
    & $javaPath @options -jar $launcher @SbtCommands
    $result = $LASTEXITCODE
} finally {
    $env:COURSIER_CACHE = $previousCache
    Pop-Location
}
exit $result
