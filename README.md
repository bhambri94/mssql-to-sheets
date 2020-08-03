### mssql-to-sheets

This utility can be used to fetch reports from Microsoft SQL Server to Google Sheets. 
This script can be used with a cron setup and which pulls out the daily report and pushes to set Google Sheet.


```
git clone https://github.com/bhambri94/mssql-to-sheets.git

cd mssql-to-sheets/

vi config.json 
//Change the configs with spreadhseet id and DB creds

docker build -t mssql-to-sheets:v1.0 .

docker images

docker run -it --name mssql-to-sheets -v $PWD/src:/go/src/mssql-to-sheets mssql-to-sheets:v1.0

```

While we run this project for the first time, we would need Google Account Access Token and Refresh Token. We need to enter a code for the first time while running this project.

Once you run the `docker run` last command shared above, a link will be displayed in the command line, which we need to open in a browser, it will ask for `Allow message` to use your account and grant access to write in the Google Sheet. Once we click the Allow button, a code would be generated, that we need to paste in console, after successful verification a token.json file will be generated at the root directory of the project. 
Note: This file will need to be regenerated if we have created a new Docker build.

#### Cron job

To setup a Daily Cron job, please follow following steps:
 
```
cd mssql-to-sheets/

Vi bash.sh

```
```
#!/bin/bash
sudo /usr/bin/docker restart mssql-to-sheets
```

Save the sheet script and run command 

```
chmod 777 bash.sh

Crontab -e

0 */12 * * * /path_to_mssql-to-sheets_repo/bash.sh > /path_to_mssql-to-sheets_repo/mssql-to-sheets.logs

```
This above command written in crontab will run the script daily twice.
