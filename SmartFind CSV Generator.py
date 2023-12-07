"""Big script to handle syncing staff information from PS into SmartFind Express.

Handles the initial profile creation - P1ProfileBasic
Also does single sign on information - W2SSO
Finally has the staff position and work day info - E1PlusEmployeeWorkSchedule
Pulls info from the teachers "table", schoolstaff, and the custom school staff extension table
Tries to be smart about adding/changing staff members by keeping track of the most recent DCID value it has processed,
though that seems uneccessary and the newest staff members dont always import anyways due to missing info.
"""

# importing modules
import datetime  # used to get current date for course info
import os  # needed to get environement variables
from datetime import *

import oracledb  # used to connect to PowerSchool database
import pysftp  # used to connect to the Performance Matters sftp site and upload the file

un = 'PSNavigator'  # PSNavigator is read only, PS is read/write
pw = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the PSNavigator account
cs = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to
#set up sftp login info, stored as environment variables on system
sftp_un = os.environ.get('SFE_SFTP_USERNAME')
sftp_host = os.environ.get('SFE_SFTP_ADDRESS')
cnopts = pysftp.CnOpts(knownhosts='known_hosts')  # connection options to use the known_hosts file for key validation

print(f'Username: {un} | Password: {pw} | Server: {cs}')  # debug so we can see where oracle is trying to connect to/with
print(f'SFTP Username: {sftp_un} | SFTP Server: {sftp_host}')  # debug so we can see what info sftp connection is using
badnames = ['use', 'training1','trianing2','trianing3','trianing4','planning','admin','nurse','user','user ','payroll', 'human', 'benefits', 'test', 'testtt', 'do not', 'plugin', 'mba']

class InvalidAccessIDError(Exception):
	"""Raised when the accessID is greater than 9 characters."""

	pass

if __name__ == '__main__':  # main file execution
	with oracledb.connect(user=un, password=pw, dsn=cs) as con:  # create the connecton to the database
		with con.cursor() as cur:  # start an entry cursor
			with open('sfe_log.txt', 'w', encoding='utf-8') as log:  # open the log output file
				print("Connection established: " + con.version)
				print("Connection established: " + con.version, file=log)
				try:
					with open('P1ProfileBasic.csv', 'w', encoding='utf-8') as outputP1:  # open the main P1 profile csv file that will be uploaded
						with open('W2SSO.csv', 'w', encoding='utf-8') as outputW2:  # open another csv file for the W2 file that controls the single sign on
							with open('E1PlusEmployeeWorkSchedule.csv', 'w', encoding='utf-8') as outputE1:
								with open('NewestDCID.txt', 'r+', encoding='utf-8') as inputDCIDFile:  # open the file that contains the most recent "new" DCID
									newestDCID = int(inputDCIDFile.readline())  # get the DCID of the most recently added person from the last run by reading the newestDCID file
									print(newestDCID)  # debug
									cur = con.cursor()  # create a cursor into the database

									cur.execute('SELECT email_addr, teachernumber, home_phone, first_name, last_name, homeschoolid, schoolid, status, users_dcid FROM teachers WHERE email_addr IS NOT NULL AND NOT homeschoolid = 2 ORDER BY users_dcid')
									users = cur.fetchall()  # store the data from the query into the rows variable

									# print('Record Type,Record Command,Record Number,Access,Telephone,First Name,Last Name,Address Line 1,Address Line 2,City,State,Zip,ExternalID,PIN,Gender,Ethnicity,Language,Email Address,IsEmployee,EmployeeActive,EmpCalendarCode,EmpBudgetCode,EmpIsItinerant,IsSub,SubActive,SubAvailable,SubAvailableLongTerm,SubCertified,SubAvailableGeneral,SubCallbackNum,SubLevel,SubPayRate,SubWorkUnits,SubOverlapping,IsAdmin,AdminActive,Restricted,Enable,Closing', file=outputfile) # debug header row

									for user in users:  # go through each user in the users result from our query. Each is a single employee's data
										try:  # put each user in a try/except block so we can just skip individual users if they have errors
											homeschool = user[5]
											firstName = user[3]
											lastName = user[4]
											if homeschool == user[6]:  # check the homeschoolid against school id, only continue if they match so we process their homeschool data (active, staff type)
												if firstName.lower() not in badnames and lastName.lower() not in badnames:  # check first and last name against array of bad names, only continue if both come back not in it
													accessID = int(user[1])
													telephoneNum = user[2] if user[2] is not None else ""  # condensed if-else, pull the telephone number if it exists otherwise leave blank
													emailAddr = user[0]
													empDCID = user[8]
													userStatus = user[7]
													# reset the following variables to None each to prevent carryover between users since some old employees might not have the HR table filled out
													amHalfTime = None
													amHalfTime2 = None
													pmHalfTime = None
													pmHalfTime2 = None
													startTime = None
													startTime2 = None
													endTime = None
													endTime2 = None
													classificationCode = None
													contractedEmployee = 0
													availNewJobs = ""  # set this variable only used for new subs to blank, will be set only if user is a sub and new

													# look at the newestDCID in our logging file, if the employee dcid is less than or equal to that they are already added and just need a change
													addOrChange = "C" if empDCID <= newestDCID else "A"

													print(f'DBUG: Processing user {emailAddr} - DCID {empDCID} - Access id {accessID}: {firstName} {lastName} at homeschool {homeschool}, phone number "{telephoneNum}" and status {userStatus}')  # debug logging
													print(f'DBUG: Processing user {emailAddr} - DCID {empDCID} - Access id {accessID}: {firstName} {lastName} at homeschool {homeschool}, phone number "{telephoneNum}" and status {userStatus}', file=log)  # debug logging

													# check to see if their accessID is a numerical value that is between 1 and 9 digits long, and is not 0. Raise an exception if not
													if accessID == 0 or (len(str(accessID)) > 9):
														raise InvalidAccessIDError
													# check if the user is a substitute teacher or not, set the sub variables accordingly
													if homeschool == 500:  # check their homeschoolid, 500 is the sub building in our PS system
														isEmployee = "N"  # set them to not be an employee (different than sub)
														empActive = "N"  # set the employee active field to no so it de-activates them as an employee
														isSub = "Y"
														empCalendarCode = ""  # subs have no calendar codes
														subActive = "Y" if userStatus == 1 else "N"  # check the teachers.status field to see if they are active
														availNewJobs = "Y" if addOrChange == "A" else ""  # if the sub is newly added, the available for new jobs column should be a Y, otherwise blank
													else:  # this means they are not a sub
														isEmployee = "Y"
														isSub = ""  # set the sub field to null so it does not print and overwrite what is currently there
														subActive = ""  # set the sub active field to null so it does not print and overwrite what is currently there
														empActive = "Y" if userStatus == 1 else "N"  # check the teachers.status field to see if they are active at the homeschool. Presumably if they are inactive at the homeschool they should be inactive everywhere

														try:  # new try/except block for the second query
															# if they are not a sub, they should have their schedule put into the custom HR table, so do another query using their DCID to retrieve it
															cur.execute('SELECT calendar, am_time, pm_time, contractemp, sfe_position, start_time, end_time, start_time_2, end_time_2, custom_times, time1_monday, time1_tuesday, time1_wednesday, time1_thursday, time1_friday, time2_monday, time2_tuesday, time2_wednesday, time2_thursday, time2_friday, am_time_2, pm_time_2 from u_humanresources WHERE usersdcid = :dcid', dcid=empDCID)  # query hr table for all the times, schedules etc, using bind variables as best practice: https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html#bind
															hrData = cur.fetchall()
															# print(hrData) # debug to print what we get back from a query of u_def_ext_schoolstaff table with that schoolstaffdcid
															if hrData:  # check to see if there is a result (hrData) since old staff may not have it
																amHalfTime = str(hrData[0][1]) if hrData[0][1] is not None else ""
																amHalfTime2 = str(hrData[0][20]) if hrData[0][1] is not None else ""
																pmHalfTime = str(hrData[0][2]) if hrData[0][2] is not None else ""
																pmHalfTime2 = str(hrData[0][21]) if hrData[0][2] is not None else ""
																contractedEmployee = hrData[0][3] if hrData[0][3] is not None else 0
																classificationCode = hrData[0][4] if hrData[0][4] is not None else None
																startTime = str(hrData[0][5]) if hrData[0][5] is not None else None
																endTime = str(hrData[0][6]) if hrData[0][6] is not None else None
																startTime2 = str(hrData[0][7]) if hrData[0][7] is not None else None
																endTime2 = str(hrData[0][8]) if hrData[0][8] is not None else None
																allowCustomTime = 'Y' if hrData[0][9] == 1 else 'N'  # new field allowing them to have custom times if the box is checked, otherwise not
																# get the checkboxes that control whether the start/end times apply to each day. Should be 0 or 1 in PS, set to Y if 1, if 0 or null set it to N
																time1Monday = 'Y' if hrData[0][10] == 1 else 'N'
																time1Tuesday = 'Y' if hrData[0][11] == 1 else 'N'
																time1Wednesday = 'Y' if hrData[0][12] == 1 else 'N'
																time1Thursday = 'Y' if hrData[0][13] == 1 else 'N'
																time1Friday = 'Y' if hrData[0][14] == 1 else 'N'
																# do the same as above but for the 2nd time entries
																time2Monday = 'Y' if hrData[0][15] == 1 else 'N'
																time2Tuesday = 'Y' if hrData[0][16] == 1 else 'N'
																time2Wednesday = 'Y' if hrData[0][17] == 1 else 'N'
																time2Thursday = 'Y' if hrData[0][18] == 1 else 'N'
																time2Friday = 'Y' if hrData[0][19] == 1 else 'N'

																# allowCustomTime = 'N'
																if hrData[0][0] is not None:
																	empCalendarCode = str(hrData[0][0])  # if the employee calendar code exists in PS, just put use it
																else:  # otherwise if the calendar code is null, we either want a blank, or to give a generic if the account is disabled since it may be old and not have a code
																	if empActive == "N":
																		empCalendarCode = "100"  # in place due to old accounts that otherwise would not import
																	else:
																		empCalendarCode = ""
															else:  # this will fill in old staff who dont have the custom data entered with just a blank
																# print("Error, custom school staff table found for " + firstName + " " +lastName) #debug
																if empActive == "N":
																	empCalendarCode = "100"
																else:
																	empCalendarCode = ""
															# print(empCalendarCode)  # debug to print the final calendar code
															if empCalendarCode == "" and addOrChange == "A":  # if they are a new addition and dont actually have a calendar code, we want to just default them to the teacher one
																empCalendarCode = "100"
														except Exception as er:
															print(f'ERROR during schedule retrieval for {emailAddr} - {empDCID}: {er}')
															print(f'ERROR during schedule retrieval for {emailAddr} - {empDCID}: {er}', file=log)
													try:  # new try/except block for writing to the files
														if (contractedEmployee != 1):
															print(f'P|{addOrChange}|1|{accessID}|{telephoneNum}|{firstName}|{lastName}|||||||||||{emailAddr}|{isEmployee}|{empActive}|{empCalendarCode}|||{isSub}|{subActive}|{availNewJobs}||||||||||||Y|', file=outputP1)  # output to the p1 profile basic file
															print(f'W|{addOrChange}|2|1|{accessID}|{emailAddr}|', file=outputW2)  # output the W2 SSO file
															if (classificationCode and startTime and endTime):  # only try to output to the E1 file if they have the 3 required pieces of data
																if "Y" in (time2Monday, time2Tuesday, time2Wednesday, time2Thursday, time2Friday):  # check to see if any of the time 2 boxes are checked meaning they have different schedules per day
																	# print(f'Found user with multiple schedules: {firstName} {lastName}')  # debug
																	# in the case they have multiple schedules, they get multiple lines on the E1 file, with the different days filled out Y/N depending on when the schedules apply
																	print(f'E|{addOrChange}|1|{accessID}|1|{homeschool}|1|{classificationCode}|{startTime}|{endTime}|N|N{time1Monday}{time1Tuesday}{time1Wednesday}{time1Thursday}{time1Friday}N|{amHalfTime}|{pmHalfTime}|{allowCustomTime}|', file=outputE1)  # print the first schedule on the first line wtih order code 1
																	print(f'E|{addOrChange}|1|{accessID}|2|{homeschool}|1|{classificationCode}|{startTime2}|{endTime2}|N|N{time2Monday}{time2Tuesday}{time2Wednesday}{time2Thursday}{time2Friday}N|{amHalfTime2}|{pmHalfTime2}|{allowCustomTime}|', file=outputE1)  # print the second schedule on a second line with order code 2
																else:
																	print(f'E|{addOrChange}|1|{accessID}|1|{homeschool}|1|{classificationCode}|{startTime}|{endTime}|N|NYYYYYN|{amHalfTime}|{pmHalfTime}|{allowCustomTime}|', file=outputE1)  # output the E1 file
																# print(f'E,{addOrChange},1,{accessID},1,{homeschool},1,{classificationCode},{startTime},{endTime},N,NYYYYYN,{amHalfTime},{pmHalfTime},|', file=outpute1) #output the E1 file, no custom time field
														else:
															print(f'WARN: Skipping contract employee at homeschool {homeschool}: {firstName} {lastName}')
															print(f'WARN: Skipping contract employee at homeschool {homeschool}: {firstName} {lastName}', file=log)
													except Exception as er:
														print(f'ERROR while writing to file for {emailAddr} - {empDCID}: {er}')
														print(f'ERROR while writing to file for {emailAddr} - {empDCID}: {er}', file=log)
												else:
													print(f'WARN: Found user with "bad" name at homeschool {homeschool}: {firstName} {lastName}')
													print(f'WARN: Found user with "bad" name at homeschool {homeschool}: {firstName} {lastName}', file=log)
										except InvalidAccessIDError:
											print(f'WARN: Skipping user {emailAddr} - {empDCID}: Access ID can have up to 9 digits and cannot be 0, current one is {accessID}')
											print(f'WARN: Skipping user {emailAddr} - {empDCID}: Access ID can have up to 9 digits and cannot be 0, current one is {accessID}', file=log)
										except Exception as er:
											print(f'ERROR on user: {user[0]} - {user[8]}: {er}')
											print(f'ERROR on user: {user[0]} - {user[8]}: {er}', file=log)

									# once we are done processing all the users, write the new most recent processed user DCID to the dcid for use in the next run.
									inputDCIDFile.seek(0)  # move the file pointer to the start of the file
									inputDCIDFile.truncate()  # delete the current text
									inputDCIDFile.write(str(empDCID))  # write the last employee dcid as the newest one

				except Exception as er:
					print(f'ERROR: High Level Error: {er}')
					print(f'ERROR: High Level Error: {er}', file=log)


				# the SFE import goes in order of oldest file to newest, so we manually set the ages of the files to be in the correct order
				now = datetime.now().timestamp()
				os.utime('P1ProfileBasic.csv', (now + 60,now+60))
				os.utime('W2SSO.csv', (now + 120,now+120))
				os.utime('E1PlusEmployeeWorkSchedule.csv', (now + 180,now+180))

				# after all the files are done writing and now closed, open an sftp connection to the server and place the file on there
				with pysftp.Connection(sftp_host, username=sftp_un, private_key='private.pem', cnopts=cnopts) as sftp:  # uses a private key file to authenticate with the server, need to pass the path
					print('SFTP connection established successfully')
					print('SFTP connection established successfully', file=log)
					# print(sftp.pwd) # debug, show what folder we connected to
					# print(sftp.listdir())  # debug, show what other files/folders are in the current directory
					sftp.chdir('./upload1')  # change to the extensionfields folder
					# print(sftp.pwd) # debug, make sure out changedir worked
					# print(sftp.listdir())
					# need to include the preserve_mtime=True in the file puts below so that the file timestamps follow over for correct ordering
					sftp.put('P1ProfileBasic.csv', preserve_mtime=True)  # upload the first file onto the sftp server
					sftp.put('W2SSO.csv', preserve_mtime=True)  # upload the second file onto the sftp server
					sftp.put('E1PlusEmployeeWorkSchedule.csv', preserve_mtime=True) # upload final file
					print("Staff files placed on remote server")
					print("Staff files placed on remote server", file=log)
				log.close()  # close the log file
