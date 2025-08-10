# RankSystem WPF - Сборка и запуск (PowerShell)
# Требует PowerShell 5.1+ и .NET 6.0 SDK

param(
    [switch]$Clean,
    [switch]$Release,
    [switch]$Debug,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
RankSystem WPF - Скрипт сборки и запуска

Использование:
    .\build_and_run.ps1 [параметры]

Параметры:
    -Clean     Очистить предыдущие сборки
    -Release   Сборка в режиме Release (по умолчанию)
    -Debug     Сборка в режиме Debug
    -Help      Показать эту справку

Примеры:
    .\build_and_run.ps1                    # Сборка Release и запуск
    .\build_and_run.ps1 -Clean            # Очистка + сборка Release + запуск
    .\build_and_run.ps1 -Debug            # Сборка Debug и запуск
"@
    exit 0
}

# Проверка .NET SDK
try {
    $dotnetVersion = dotnet --version
    Write-Host "Найден .NET SDK версии: $dotnetVersion" -ForegroundColor Green
} catch {
    Write-Host "Ошибка: .NET SDK не найден!" -ForegroundColor Red
    Write-Host "Установите .NET 6.0 SDK или выше с https://dotnet.microsoft.com/" -ForegroundColor Yellow
    exit 1
}

# Определение конфигурации
$configuration = if ($Debug) { "Debug" } else { "Release" }

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "RankSystem WPF - Сборка и запуск" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Конфигурация: $configuration" -ForegroundColor Yellow
Write-Host ""

# Очистка при необходимости
if ($Clean) {
    Write-Host "Очистка предыдущих сборок..." -ForegroundColor Yellow
    if (Test-Path "bin") { Remove-Item "bin" -Recurse -Force }
    if (Test-Path "obj") { Remove-Item "obj" -Recurse -Force }
    Write-Host "Очистка завершена" -ForegroundColor Green
    Write-Host ""
}

# Восстановление пакетов
Write-Host "Восстановление пакетов NuGet..." -ForegroundColor Yellow
$restoreResult = dotnet restore
if ($LASTEXITCODE -ne 0) {
    Write-Host "Ошибка при восстановлении пакетов!" -ForegroundColor Red
    Write-Host $restoreResult -ForegroundColor Red
    Read-Host "Нажмите Enter для выхода"
    exit 1
}
Write-Host "Пакеты восстановлены" -ForegroundColor Green
Write-Host ""

# Сборка проекта
Write-Host "Сборка проекта в режиме $configuration..." -ForegroundColor Yellow
$buildResult = dotnet build --configuration $configuration --verbosity minimal
if ($LASTEXITCODE -ne 0) {
    Write-Host "Ошибка при сборке!" -ForegroundColor Red
    Write-Host $buildResult -ForegroundColor Red
    Read-Host "Нажмите Enter для выхода"
    exit 1
}
Write-Host "Сборка завершена успешно!" -ForegroundColor Green
Write-Host ""

# Запуск приложения
Write-Host "Запуск приложения..." -ForegroundColor Yellow
Write-Host "Для выхода закройте окно приложения или нажмите Ctrl+C" -ForegroundColor Cyan
Write-Host ""

try {
    dotnet run --configuration $configuration
} catch {
    Write-Host "Приложение завершено с ошибкой: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "Приложение завершено" -ForegroundColor Green
Read-Host "Нажмите Enter для выхода"
