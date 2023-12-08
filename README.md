
# D118-SmartFindExpressSync

Script to generate the required files to sync users from PowerSchool to SmartFind Express. Retrieves staff data via database queries to the teachers table and our custom human resource information table, then formats it into the .csv files and uploads it via SFTP to SmartFind Express.

## Overview

The script first does a query for all staff in PowerSchool from the teachers "table" that have an email in their profile (to skip unused accounts) and gets the basic user information about them. It then begins to go through each staff entry, checks to see if the current staff entry is their homeschool entry, checks for a valid accessID which is the teachnumber from PowerSchool, and processes if they are a substitute teacher or not.

Then it tries to look in our custom u_humanresources table for the schedule information including work calendar code, daily schedules, etc. If that data does not exist for very old users, it instead fills in the required fields with blanks or the default teacher codes. The script can now handle staff that have 2 different schedules, for instance someone who works mornings Monday Wednesday Friday and afternoons Tuesday and Thursday. If they have checkboxes checked in PowerSchool that dictate they should use schedule 2 on certain days, it splits up schedule 1 and schedule 2 to separate lines in the E1 file, and uses those checkboxes to create the NXXXXXN string for their schedules.

All the information is then output to the P1ProfileBasic.csv, W2SSO.csv, and E1PlusEmployeeWorkSchedule.csv files, in the format that follows the SmartFind import guide, with | as a delimiter (we used commas and tabs in the past but were advised to use | instead).

Finally, these files have their timestamps altered so that P1 is the oldest file, and then all 3 files are uploaded via SFTP to the SmartFind Express server they provided access to during our setup process. The files need to be re-timestamped in this way because they will be imported in order of oldest to newest files, and if you import the extra files before P1, new users will not have their basic profile created and all schedule and SS0 information will fail to import for that time, which just leads to an extra day of delay.

## Requirements

The following Environment Variables must be set on the machine running the script:

- POWERSCHOOL_READ_USER
- POWERSCHOOL_DB_PASSWORD
- POWERSCHOOL_PROD_DB
- SFE_SFTP_USERNAME
- SFE_SFTP_ADDRESS

*These are fairly self explanatory, slightly more context is provided in the script comments.*

Additionally,the following Python libraries must be installed on the host machine (links to the installation guide):

- [Python-oracledb](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html)
- [pysftp](https://pypi.org/project/pysftp/)

For the SFTP connection, you will need a private.pem file containing the private key certificate for the SmartFind Express SFTP server. This is provided to you during the onboarding and setup of the SmartFind Express system by PowerSchool. The file should be placed in the same directory as the Python script.
You will also need to include the server host public key key in a file with no extension named "known_hosts" in the same directory as the Python script. You can see [here](https://pysftp.readthedocs.io/en/release_0.2.9/cookbook.html#pysftp-cnopts) for details on how it is used, but the easiest way to include this I have found is to create an SSH connection from a Linux machine using the login info and then find the key (the newest entry should be on the bottom) in ~/.ssh/known_hosts and copy and paste that into a new file named "known_hosts" in the script directory.

The last thing you need is a text file named "NewestDCID.txt" in the same directory as the script. For the first run, open it and put a "0" on the first line, then save and close. Subsequent runs of the script will update this number to the most recently processed PowerSchool user DCID, in order to know whether the record exported to SmartFind should have a "C" for change (user exists) or "A" (user needs to be added).

## Customization

This a very specific and customized script for our specific PowerSchool and SmartFind Express setup at D118. For customization or use outside of my specific use case at D118, you will probably want to look at and edit the following items outside of the environment variables in the requirements section above:

- The query to get schedule information, work calendar, classification code, etc is completely reliant on our custom table we created named u_humanresources, and has the am/pm times, start/end times, and the checkboxes for time1/2 Monday-Fridays. We use a custom screen in PowerSchool to fill out this information as seen below. You will need to change the custom table and field names in the query to match whatever is in your PowerSchool setup.
![Screenshot](https://raw.githubusercontent.com/Philip-Greyson/D118-SmartFindExpressSync/main/SmartFInd%20Custom%20Screen.png)
- `SUB_BUILDING = 500` should be changed to match the schoolid of your substitute building in PowerSchool. If you do not have a separate building in PS for subs, you will have to modify the section where it processes whether they are a sub or not, starting with the line`if homeschool == SUB_BUILDING`
- The `badnames = [xxx]` list can be updated to include any first or last names of users that should be ignored. For example, we have many accounts used for testing that include "test" or "do not use".
- If you want to use a different directory for the SFTP upload, change the `sftp.chdir('./upload1')` line to have your directory path in the quotes. There are some commented debug lines above and below this line that can help with showing which directory you are currently in when you log in and its contents.
