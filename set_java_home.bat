@echo off
REM Script to set JAVA_HOME environment variable for QueueMonitor
REM Copyright (C) 2025 Garland Glessner - gglesner@gmail.com

echo QueueMonitor Java Setup
echo =======================
echo.

REM Check if Java is installed
where java >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Java not found! Please install Java first.
    echo You can download it from: https://www.oracle.com/java/technologies/downloads/
    echo.
    pause
    exit /b 1
)

REM Get Java version
java -version 2>&1 | findstr /i "version"
echo.

REM Look for existing JAVA_HOME
if defined JAVA_HOME (
    echo Current JAVA_HOME: %JAVA_HOME%
    echo.
)

REM Ask user if they want to set JAVA_HOME
set /p choice="Do you want to set JAVA_HOME environment variable? (Y/N): "
if /i "%choice%" NEQ "Y" goto :end

REM Ask for Java installation path
echo.
echo Enter the path to your Java installation directory
echo Example: C:\Program Files\Java\jdk-17.0.2
echo.
set /p java_path="Java path: "

REM Validate path
if not exist "%java_path%" (
    echo Error: The specified path does not exist.
    pause
    exit /b 1
)

REM Check for java.exe to validate it's a Java installation
if not exist "%java_path%\bin\java.exe" (
    if not exist "%java_path%\jre\bin\java.exe" (
        echo Error: This does not appear to be a valid Java installation.
        echo Expected to find java.exe in %java_path%\bin or %java_path%\jre\bin
        pause
        exit /b 1
    )
)

REM Set JAVA_HOME for current user
setx JAVA_HOME "%java_path%"
echo.
echo JAVA_HOME has been set to: %java_path%
echo.
echo Please restart any open command prompts or applications for the changes to take effect.

:end
echo.
echo Setup complete.
pause 