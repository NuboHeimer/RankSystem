@echo off
echo ========================================
echo RankSystem WPF - Сборка и запуск
echo ========================================
echo.

echo Очистка предыдущих сборок...
if exist "bin" rmdir /s /q "bin"
if exist "obj" rmdir /s /q "obj"
echo.

echo Восстановление пакетов NuGet...
dotnet restore
if %errorlevel% neq 0 (
    echo Ошибка при восстановлении пакетов!
    pause
    exit /b 1
)
echo.

echo Сборка проекта...
dotnet build --configuration Release
if %errorlevel% neq 0 (
    echo Ошибка при сборке!
    pause
    exit /b 1
)
echo.

echo Сборка завершена успешно!
echo.

echo Запуск приложения...
dotnet run --configuration Release
echo.

echo Приложение завершено.
pause
