@echo off
echo ==========================================
echo Creando entorno virtual con Python 3.11...
echo ==========================================

:: Crea el entorno virtual llamando específicamente a Python 3.11
py -3.11 -m venv venv_tf

if errorlevel 1 (
    echo [ERROR] No se pudo crear el entorno. Comprueba que Python 3.11 esta instalado.
    pause
    exit /b
)

echo.
echo ==========================================
echo Activando el entorno virtual y actualizando pip...
echo ==========================================

:: Activa el entorno e instala/actualiza pip
call venv_tf\Scripts\activate.bat
python -m pip install --upgrade pip

echo.
echo ==========================================
echo Instalando dependencias desde requirements.txt...
echo ==========================================

if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo [ERROR] No se encontro el archivo requirements.txt en esta carpeta.
    echo Asegurate de guardar el script .bat en la misma ruta que requirements.txt.
    pause
    exit /b
)

echo.
echo ==========================================
echo ¡Proceso finalizado con exito!
echo ==========================================
pause



