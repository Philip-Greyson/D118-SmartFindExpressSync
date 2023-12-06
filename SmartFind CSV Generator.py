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
badnames = ['USE', 'Training1','Trianing2','Trianing3','Trianing4','Planning','Admin','ADMIN','NURSE','USER', 'USE ', 'PAYROLL', 'Human', 'BENEFITS', 'TEST', 'TESTTT']

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

									cur.execute('SELECT teachers.email_addr, teachers.teachernumber, teachers.home_phone, teachers.first_name, teachers.last_name, teachers.email_addr, teachers.homeschoolid, teachers.schoolid, teachers.status, teachers.users_dcid FROM teachers WHERE teachers.email_addr IS NOT NULL AND NOT teachers.homeschoolid = 2 ORDER BY teachers.users_dcid')
									rows = cur.fetchall()  # store the data from the query into the rows variable

									# print('Record Type,Record Command,Record Number,Access,Telephone,First Name,Last Name,Address Line 1,Address Line 2,City,State,Zip,ExternalID,PIN,Gender,Ethnicity,Language,Email Address,IsEmployee,EmployeeActive,EmpCalendarCode,EmpBudgetCode,EmpIsItinerant,IsSub,SubActive,SubAvailable,SubAvailableLongTerm,SubCertified,SubAvailableGeneral,SubCallbackNum,SubLevel,SubPayRate,SubWorkUnits,SubOverlapping,IsAdmin,AdminActive,Restricted,Enable,Closing', file=outputfile) # debug header row

									for entrytuple in rows:  # go through each entry (which is a tuple) in rows. Each entrytuple is a single employee's data
										try:  # put each user in a try/except block so we can just skip individual users if they have errors
											entry = list(entrytuple)  # convert the tuple which is immutable to a list which we can edit. Now entry[] is an array/list of the employee data
											#for stuff in entry:
												#print(stuff) #debug
											homeschool = entry[6]
											if homeschool == entry[7]:  # check the homeschoolid against school id, only print if they match
												if entry[3] not in badnames and entry[4] not in badnames:  # check first and last name against array of bad names, only print if both come back not in it
													accessID = entry[1]
													telephoneNum = entry[2] if entry[2] is not None else ""  # condensed if-else, pull the telephone number if it exists otherwise leave blank
													firstName = entry[3]
													lastName = entry[4]
													emailAddr = entry[0]
													empDCID = entry[9]
													amHalfTime = None  # reset these to none each time to prevent carryover
													pmHalfTime = None
													startTime = None
													endTime = None
													classificationCode = None
													contractedEmployee = 0
													availNewJobs = ""  # set this variable only used for new subs to blank, will be set only if user is a sub and new
													addOrChange = "C" if empDCID <= newestDCID else "A"  # look at the newestDCID in our logging file, if the employee dcid is less than or equal to that they are already added and just need a change
													if entry[6] == 500:  # check their homeschoolid, 500 is the sub building in PS
														isEmployee = "N"  # set them to not be an employee (different than sub)
														empActive = "N"  # set the employee active field to no so it de-activates them as an employee
														isSub = "Y"
														empCalendarCode = ""  # subs have no calendar codes
														subActive = "Y" if entry[8] == 1 else "N"  # check the teachers.status field to see if they are active
														availNewJobs = "Y" if addOrChange == "A" else ""  # if the sub is newly added, the available for new jobs column should be a Y, otherwise blank
													else:  # this means they are not a sub
														isEmployee = "Y"
														isSub = ""  # set the sub field to null so it does not print and overwrite what is currently there
														subActive = ""  # set the sub active field to null so it does not print and overwrite what is currently there
														empActive = "Y" if entry[8] == 1 else "N"  # check the teachers.status field to see if they are active at the homeschool. Presumably if they are inactive at the homeschool they should be inactive everywhere

														# print(schoolStaffRows) #debug to print result of query of schoolstaff table with the normal dcid
														# print(schoolStaffDCID) #debug to print actual schoolstaffdcid

														cur.execute(f'SELECT calendar, am_time, pm_time, contractemp, sfe_position, start_time, end_time, start_time_2, end_time_2, custom_times, time1_monday, time1_tuesday, time1_wednesday, time1_thursday, time1_friday, time2_monday, time2_tuesday, time2_wednesday, time2_thursday, time2_friday from u_humanresources WHERE usersdcid = {empDCID}')  # updated for new hr table extension of users
														cusSchoolStaffRows = cur.fetchall()

														# print(cusSchoolStaffRows) # debug to print what we get back from a query of u_def_ext_schoolstaff table with that schoolstaffdcid

														if cusSchoolStaffRows:  # check to see if there is a result (cusSchoolStaffRows) since old staff may not have it
															amHalfTime = str(cusSchoolStaffRows[0][1]) if cusSchoolStaffRows[0][1] is not None else None
															pmHalfTime = str(cusSchoolStaffRows[0][2]) if cusSchoolStaffRows[0][2] is not None else None
															contractedEmployee = cusSchoolStaffRows[0][3] if cusSchoolStaffRows[0][3] is not None else 0
															classificationCode = cusSchoolStaffRows[0][4] if cusSchoolStaffRows[0][4] is not None else None
															startTime = str(cusSchoolStaffRows[0][5]) if cusSchoolStaffRows[0][5] is not None else None
															endTime = str(cusSchoolStaffRows[0][6]) if cusSchoolStaffRows[0][6] is not None else None
															startTime2 = str(cusSchoolStaffRows[0][7]) if cusSchoolStaffRows[0][7] is not None else None
															endTime2 = str(cusSchoolStaffRows[0][8]) if cusSchoolStaffRows[0][8] is not None else None
															allowCustomTime = 'Y' if cusSchoolStaffRows[0][9] == 1 else 'N'  # new field allowing them to have custom times if the box is checked, otherwise not
															# get the checkboxes that control whether the start/end times apply to each day. Should be 0 or 1, if its null treat it as 0
															time1Monday = int(cusSchoolStaffRows[0][10]) if cusSchoolStaffRows[0][10] is not None else 0
															time1Tuesday = int(cusSchoolStaffRows[0][11]) if cusSchoolStaffRows[0][11] is not None else 0
															time1Wednesday = int(cusSchoolStaffRows[0][12]) if cusSchoolStaffRows[0][12] is not None else 0
															time1Thursday = int(cusSchoolStaffRows[0][13]) if cusSchoolStaffRows[0][13] is not None else 0
															time1Friday = int(cusSchoolStaffRows[0][14]) if cusSchoolStaffRows[0][14] is not None else 0
															# do the same as above but for the 2nd time entries
															time2Monday = int(cusSchoolStaffRows[0][15]) if cusSchoolStaffRows[0][15] is not None else 0
															time2Tuesday = int(cusSchoolStaffRows[0][16]) if cusSchoolStaffRows[0][16] is not None else 0
															time2Wednesday = int(cusSchoolStaffRows[0][17]) if cusSchoolStaffRows[0][17] is not None else 0
															time2Thursday = int(cusSchoolStaffRows[0][18]) if cusSchoolStaffRows[0][18] is not None else 0
															time2Friday = int(cusSchoolStaffRows[0][19]) if cusSchoolStaffRows[0][19] is not None else 0

															# allowCustomTime = 'N'
															if cusSchoolStaffRows[0][0] is not None:
																empCalendarCode = str(cusSchoolStaffRows[0][0])  # if the employee calendar code exists in PS, just put use it
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
													if (contractedEmployee != 1):
														print(f'P|{addOrChange}|1|{accessID}|{telephoneNum}|{firstName}|{lastName}|||||||||||{emailAddr}|{isEmployee}|{empActive}|{empCalendarCode}|||{isSub}|{subActive}|{availNewJobs}||||||||||||Y|', file=outputP1)  # output to the p1 profile basic file
														print(f'W|{addOrChange}|2|1|{accessID}|{emailAddr}|', file=outputW2)  # output the W2 SSO file
														if (amHalfTime and pmHalfTime and classificationCode and startTime and endTime):  # only try to output to the E1 file if they have the 3 required pieces of data
															print(f'E|{addOrChange}|1|{accessID}|1|{homeschool}|1|{classificationCode}|{startTime}|{endTime}|N|NYYYYYN|{amHalfTime}|{pmHalfTime}|{allowCustomTime}|', file=outputE1)  # output the E1 file
															# print(f'E,{addOrChange},1,{accessID},1,{homeschool},1,{classificationCode},{startTime},{endTime},N,NYYYYYN,{amHalfTime},{pmHalfTime},|', file=outpute1) #output the E1 file, no custom time field
													else:
														print("Skipping contract employee " + firstName + " " + lastName)
										except Exception as er:
											print(f'ERROR on user: {entry[9]}: {er}')
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
