@echo off
echo ======================================================================
echo Creating Virtual Envireonment...
echo ======================================================================
python -m venv venv && call venv\Scripts\activate
echo:
echo ======================================================================
echo Virtual Envireonment Activated!
echo ======================================================================
echo:
echo ======================================================================
echo Checking installed requirements...
echo ======================================================================
echo:
python -m pip install --upgrade pip
python -m pip install -q -r requirements.txt
echo ======================================================================
echo Done!
echo ======================================================================
echo:
cmd /k streamlit run main.py