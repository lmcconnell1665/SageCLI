[![Build and test project](https://github.com/lmcconnell1665/SageCLI/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/lmcconnell1665/SageCLI/actions/workflows/build-and-test.yml)

# SageCLI
Quick and dirty CLI for bulk extracting data from Sage Intacct and saving in Azure storage. Save them in a `.env` file for debugging or use `export` from the terminal to declare them (I'm using Ubuntu running in WSL2).

# Virtual Environment
This code will run most effectively in a virtual environment. Set one up by:

1. Running `make setup` from the top directory of the project to create a virtual environment
2. Running `source ~/.SageCLI/bin/activate` to activate the virtual environment
3. Running `make install` to install the package dependencies

# Environmental Varables
The following environment variables are required for the tool to work properly:

| Variable Name | Purpose |
| ------------- | ------- |
| SAGE_USER_PASSWORD | API user password for connecting to Sage Intacct | 
| SAGE_COMPANY_ID | API company id for connecting to Sage Intacct |
| SAGE_USER_ID | API user id for connecting to Sage Intacct |
| SAGE_SENDER_ID | API sender id for connecting to Sage Intact |
| SAGE_SENDER_PASSWORD | API sender password for connecting to Sage Intacct |
| AZURE_STORAGE_ACCT_NAME | Azure storage account name that you want to save the data in |
| AZURE_STORAGE_ACCT_KEY | Azure storage account access key (with no prefixes) |
| AZURE_STORAGE_FILESYSTEM | Azure storage account container you want to save the data in (will create a `Sage_Intacct/data_download` subdirectory in this container) |

# Run Commands
From the working directory, run `python3 sage_slasher.py --help` to see the list of commands.

Run `python3 sage_slasher.py {command-name}` to start an operation. You will be prompted for information like the `entity` you want to extract or the `data range` that needs to be pulled.

**Data  Entities:** Entity names mimic object parameter names in the [Sage Intacct docs.](https://developer.intacct.com/api/company-console/entities/)

**Date Ranges:** Date ranges are exclusive on the right-hand side because it made it eay to handle varying number of days in months. If you want data for all of November 2022, use a start date of `2022-11-01` and an end date of `2022-12-01`. 

# View Logs
A log of all events is saved locally in the `sage.log` file. If the `full-extract` command is used, an additional .csv file will be generated, stamped with the entity and start time of the run containing summary statistics for each month. This can be helpful to know where the start if a long-running operation breaks halfway through due to a connection failure.
