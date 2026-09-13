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

:: Activa el entorno e instala las dependencias
call venv_tf\Scripts\activate.bat
python -m pip install --upgrade pip

echo.
echo ==========================================
echo Instalando TensorFlow...
echo ==========================================
pip install tensorflow


echo.
echo ==========================================
echo Instalando Pandas y mas cosas
echo ==========================================
pip install pandas matplotlib numpy scikit-learn python-dotenv sqlalchemy joblib jupyterlab psycopg2


echo.
echo ==========================================
echo ¡Proceso finalizado con exito!
echo ==========================================
pause