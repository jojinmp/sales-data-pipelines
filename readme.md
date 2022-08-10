## File/folder attached:
- main.py: Its the entry point for the program
- requirements.txt: Contains the list of required libraries
- module_package: Custom package containing all the modules.
    * load_data: Has modules for json to parquete conversion and to plot diagrams.
    * transformations: Contains all the transformation modules
- dataset: Provided dataset,the program will create a sub directory-output for storing parquet files.
- test_data: Dataset used for unit testing.
- tests: Contains unit_test.py for unit testing.
- setup.py: Place holder for now, would be required during packaging.
- log.txt: Logging file

## Instructions to run:

1. Make sure latest version of python is installed, if not install it from https://www.python.org/downloads/
2. Open command prompt or any IDE then navigate to Pipelines folder and install required libraries using below command.

    `pip install -r requirements.txt`
3. Run the test cases using `pytest` command
4. Run the main program by typing `main.py` in command prompt.


