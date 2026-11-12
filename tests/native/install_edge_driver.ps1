# Match the installed WebView2 runtime's first three version components.
$ErrorActionPreference = 'Stop'
$roots = @(${env:ProgramFiles(x86)}, $env:ProgramFiles, $env:LOCALAPPDATA) | Where-Object { $_ }
$runtimes = foreach ($root in $roots) {
  Get-ChildItem (Join-Path $root 'Microsoft/EdgeWebView/Application/*/msedgewebview2.exe') -ErrorAction SilentlyContinue
}
$runtime = $runtimes | Sort-Object { [version]$_.VersionInfo.ProductVersion } -Descending | Select-Object -First 1
if (!$runtime) { throw 'WebView2 runtime is required for the real Windows native smoke test.' }
$version = $runtime.VersionInfo.ProductVersion
$build = ($version.Split('.')[0..2] -join '.')
$directory = Join-Path $env:RUNNER_TEMP 'trivium-edgedriver'
New-Item -ItemType Directory -Path $directory -Force | Out-Null
$archive = Join-Path $directory 'driver.zip'
try {
  Invoke-WebRequest "https://msedgedriver.microsoft.com/$version/edgedriver_win64.zip" -OutFile $archive
} catch {
  # Driver/runtime patch releases can differ. Only accept the same build.
  $major = $version.Split('.')[0]
  $latest = Join-Path $directory 'latest.txt'
  Invoke-WebRequest "https://msedgedriver.microsoft.com/LATEST_RELEASE_${major}_WINDOWS" -OutFile $latest
  $driverVersion = ([System.IO.File]::ReadAllText($latest) -replace "`0", '').Trim()
  if (!$driverVersion.StartsWith("$build.")) { throw "WebView2 $version has no matching Edge driver build; found $driverVersion" }
  Invoke-WebRequest "https://msedgedriver.microsoft.com/$driverVersion/edgedriver_win64.zip" -OutFile $archive
}
Expand-Archive $archive -DestinationPath $directory -Force
$driver = Join-Path $directory 'msedgedriver.exe'
$driverInfo = (& $driver --version) -join ' '
if ($driverInfo -notmatch "\b$([regex]::Escape($build))\.[0-9]+\b") { throw "Driver mismatch: WebView2 $version, $driverInfo" }
Write-Host "WebView2 runtime: $version; native driver: $driverInfo"
"TRIVIUM_NATIVE_DRIVER=$driver" | Out-File $env:GITHUB_ENV -Append -Encoding utf8
