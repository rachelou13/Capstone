# Capstone
TEKSystems Early Career SRE Bootcamp Capstone Project

# Python Packages
This section will cover installing the required Python packages. The following instructions use prompts for a Git Bash terminal on Windows OS. Other environments may use slightly different commands. Before getting started ensure you have the most up-to-date version of pip installed by running the command:
```
python -m pip install --upgrade pip
```

## Installing Required Python Packages Locally (Option 1)
1. Navigate to the main capstone folder on your local machine
2. Run ```ls``` to ensure that you are in the folder containing requirements.txt
3. Run ```pip install -r requirements.txt```

## Using a Python Virtual Environment (Option 2)
Virtual environments help keep Python packages contained to the project where they are relevant. If you wish to clean up existing Python packages from your machine, you can run the following commands.
```
pip freeze > packages.txt
pip uninstall -r packages.txt -y
```
### Python Virtual Environment Setup (Command Line)
1. Navigate to the main capstone folder on your local machine
2. Run ```ls``` to ensure that you are in the folder containing requirements.txt
3. Run ```python -m venv .venv```
4. Run ```source .venv/Scripts/activate```
5. You are now in your virtual environment and should see (.venv) at the start of each line in your terminal
6. Run ```python -m pip install -r requirements.txt``` \
    a. You may use ```pip install -r requirements.txt``` however, using ```python -m``` as a preface ensures you are using the ```pip``` associated with the currently active Python interpreter (the one inside the virtual environment). This is a good practice. 
7. Check the correct packages are installed by running ```python -m pip freeze```
#### Setting Your Virtual Environment in VS Code
1. Open your main capstone folder in VS Code
2. Open the Command Palette with the shortcut Ctrl+Shift+P
3. Search for and select 'Python: Select Interpreter'
4. Select 'Enter interpreter path...'
5. Select 'Find...'
6. Browse to '.venv\Scripts' within your capstone folder and select 'python.exe'
#### Exiting the Virtual Environment
1. When you are done working in the virtual environment, simply run ```deactivate``` in your terminal
## Updating requirements.txt
If you add additional required Python packages that all users should have installed, follow the below instructions to update the requirements.txt document
1. If you are using a virtual environemt, ensure it is active
2. Run ```python -m pip freeze``` and review the list of Python packages
3. Run ```python -m pip freeze > requiremnts.txt``` \
    ***Note: This command completely replaces the current list of requirements with whatever you currently have installed, ensure you have all requirements installed before running this***
4. (Optional) Run ```cat requirements.txt``` to ensure your changes were written successfully