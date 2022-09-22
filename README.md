# D118-SmartFindExpressSync
Script to generate all required files to sync users from PowerSchool to SmartFind Express

Written in Python 3.10.4 
Need to have oracledb and pysftb libraries installed 

Need to have the privatekey for connecting to the SFE SFTP server saved as private.pem in the working directory 

Also need to have the host added to a known_hosts file in the same directory 

Finally a NewestDCID.txt file to track the most recent processed user 