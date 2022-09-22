# Big script to handle syncing staff information from PS into SmartFind Express
# Handles the initial profile creation - P1ProfileBasic
# Also does single sign on information - W2SSO
# Finally has the staff position and work day info - E1PlusEmployeeWorkSchedule
# Pulls info from the teachers "table", schoolstaff, and the custom school staff extension table
# Tries to be smart about adding/changing staff members by keeping track of the most recent DCID value it has processed,
# 	though that seems uneccessary and the newest staff members dont always import anyways due to missing info

# importing modules
import oracledb  # used to connect to PowerSchool database
import datetime  # used to get current date for course info
import os  # needed to get environement variables
import pysftp  # used to connect to the Performance Matters sftp site and upload the file
from datetime import *

un = 'PSNavigator' #PSNavigator is read only, PS is read/write
pw = os.environ.get('POWERSCHOOL_DB_PASSWORD') #the password for the PSNavigator account
cs = os.environ.get('POWERSCHOOL_PROD_DB') #the IP address, port, and database name to connect to
#set up sftp login info, stored as environment variables on system
sftpUN = os.environ.get('SFE_SFTP_USERNAME')
sftpHOST = os.environ.get('SFE_SFTP_ADDRESS')
cnopts = pysftp.CnOpts(knownhosts='known_hosts') #connection options to use the known_hosts file for key validation

print("Username: " + str(un) + " |Password: " + str(pw) + " |Server: " + str(cs)) #debug so we can see where oracle is trying to connect to/with
print("SFTP Username: " + str(sftpUN) +  " |SFTP Server: " + str(sftpHOST)) #debug so we can see what info sftp connection is using
badnames = ['USE', 'Training1','Trianing2','Trianing3','Trianing4','Planning','Admin','ADMIN','NURSE','USER', 'USE ', 'PAYROLL', 'Human', 'BENEFITS', 'TEST', 'TESTTT']

with oracledb.connect(user=un, password=pw, dsn=cs) as con: # create the connecton to the database
	with con.cursor() as cur:  # start an entry cursor
		with open('P1ProfileBasic.csv', 'w') as outputfile:
			try:
				outputLog = open('sfe_log.txt', 'w') #open a second file for the log output
				print("Connection established: " + con.version)
				print("Connection established: " + con.version, file=outputLog)
				outputw2 = open('W2SSO.csv', 'w') #open a second file for the w2 sso fields
				outpute1 = open('E1PlusEmployeeWorkSchedule.csv', 'w')
				inputDCIDFile = open('NewestDCID.txt', 'r+') #open the file that contains the most recent "new" DCID
				newestDCID = int(inputDCIDFile.readline())
				print(newestDCID) #debug
				cur = con.cursor()
				# fetchall() is used to fetch all records from result set
				cur.execute('SELECT teachers.email_addr, teachers.teachernumber, teachers.home_phone, teachers.first_name, teachers.last_name, teachers.email_addr, teachers.homeschoolid, teachers.schoolid, teachers.status, teachers.users_dcid FROM teachers WHERE teachers.email_addr IS NOT NULL AND NOT teachers.homeschoolid =2 ORDER BY teachers.users_dcid')

				rows = cur.fetchall() #store the data from the query into the rows variable

				#print('Record Type,Record Command,Record Number,Access,Telephone,First Name,Last Name,Address Line 1,Address Line 2,City,State,Zip,ExternalID,PIN,Gender,Ethnicity,Email Address,Language,IsEmployee,EmployeeActive,EmpCalendarCode,EmpBudgetCode,EmpIsItinerant,IsSub,SubActive,SubAvailable,SubAvailableLongTerm,SubCertified,SubAvailableGeneral,SubCallbackNum,SubLevel,SubPayRate,SubWorkUnits,SubOverlapping,IsAdmin,AdminActive,Restricted,Enable,Closing', file=outputfile)
				for entrytuple in rows: #go through each entry (which is a tuple) in rows. Each entrytuple is a single employee's data
					try: # put each user in a try/except block so we can just skip individual users if they have errors
						entry = list(entrytuple) #convert the tuple which is immutable to a list which we can edit. Now entry[] is an array/list of the employee data
						#for stuff in entry:
							#print(stuff) #debug
						homeschool = entry[6]
						if homeschool == entry[7]: #check the homeschoolid against school id, only print if they match
							if not entry[3] in badnames and not entry[4] in badnames: #check first and last name against array of bad names, only print if both come back not in it
								accessID = entry[1]
								telephoneNum = entry[2] if entry[2] != None else "" #condensed if-else, pull the telephone number if it exists otherwise leave blank
								firstName = entry[3]
								lastName = entry[4]
								emailAddr = entry[0]
								empDCID = entry[9]
								amHalfTime = None #reset these to none each time to prevent carryover
								pmHalfTime = None
								startTime = None
								endTime = None
								classificationCode = None
								contractedEmployee = 0
								availNewJobs = "" #set this variable only used for new subs to blank, will be set only if user is a sub and new
								addOrChange = "C" if empDCID <= newestDCID else "A" #look at the newestDCID in our logging file, if the employee dcid is less than or equal to that they are already added and just need a change
								if entry[6] == 500: #check their homeschoolid, 500 is the sub building in PS
									isEmployee = "N" #set them to not be an employee (different than sub)
									empActive = "N" #set the employee active field to no so it de-activates them as an employee
									isSub = "Y"
									empCalendarCode = "" #subs have no calendar codes
									subActive = "Y" if entry[8] == 1 else "N" #check the teachers.status field to see if they are active
									availNewJobs = "Y" if addOrChange == "A" else "" #if the sub is newly added, the available for new jobs column should be a Y, otherwise blank
								else: #this means they are not a sub
									isEmployee = "Y"
									isSub = "" #set the sub field to null so it does not print and overwrite what is currently there
									subActive = "" #set the sub active field to null so it does not print and overwrite what is currently there
									empActive = "Y" if entry[8] == 1 else "N"  #check the teachers.status field to see if they are active
									cur.execute('SELECT dcid FROM schoolstaff WHERE users_dcid = ' +str(empDCID)+ 'AND schoolid = ' +str(homeschool)) #do a new query on the schoolStaff table matching the user dcid and homeschool to get the schoolstaff entry dcid
									schoolStaffRows = cur.fetchall()
									schoolStaffDCID = str(schoolStaffRows[0][0]) if schoolStaffRows else "" #check to see if there is a result (schoolStaffRows) since old staff may not have it
									#print(schoolStaffRows) #debug to print result of query of schoolstaff table with the normal dcid
									#print(schoolStaffDCID) #debug to print actual schoolstaffdcid

									cur.execute('SELECT hr_calendar, sfe_am_time, sfe_pm_time, hr_contractemp, hr_sfe_position, sfe_start_time, sfe_end_time FROM u_def_ext_schoolstaff WHERE schoolstaffdcid = ' +str(schoolStaffDCID)) #take the entry dcid from schoolstaff table and pass to custom school staff table to get hr calender
									cusSchoolStaffRows = cur.fetchall()
									#print(cusSchoolStaffRows) #debug to print what we get back from a query of u_def_ext_schoolstaff table with that schoolstaffdcid
									if cusSchoolStaffRows: #check to see if there is a result (cusSchoolStaffRows) since old staff may not have it
										amHalfTime = cusSchoolStaffRows[0][1] if cusSchoolStaffRows[0][1] != None else None
										pmHalfTime = cusSchoolStaffRows[0][2] if cusSchoolStaffRows[0][2] != None else None
										contractedEmployee = cusSchoolStaffRows[0][3] if cusSchoolStaffRows[0][3] != None else 0
										classificationCode = cusSchoolStaffRows[0][4] if cusSchoolStaffRows[0][4] != None else None
										startTime = cusSchoolStaffRows[0][5] if cusSchoolStaffRows[0][5] != None else None
										endTime = cusSchoolStaffRows[0][6] if cusSchoolStaffRows[0][6] != None else None
										if cusSchoolStaffRows[0][0]!= None:
											empCalendarCode = str(cusSchoolStaffRows[0][0]) #if the employee calendar code exists in PS, just put use it
										else: #otherwise if the calendar code is null, we either want a blank, or to give a generic if the account is disabled since it may be old and not have a code
											if empActive == "N":
												empCalendarCode = "100" #in place due to old accounts that otherwise would not import
											else:
												empCalendarCode = ""
									else: #this will fill in old staff who dont have the custom data entered with just a blank
										#print("Error, custom school staff table found for " + firstName + " " +lastName) #debug
										if empActive == "N":
											empCalendarCode = "100"
										else:
											empCalendarCode = ""
									#print(empCalendarCode) #debug to print the final calendar code
									if empCalendarCode == "" and addOrChange == "A": #if they are a new addition and dont actually have a calendar code, we want to just default them to the teacher one
										empCalendarCode = "100"
								if (contractedEmployee != 1):
									print('P,'+addOrChange+',1,'+accessID+','+telephoneNum+','+firstName+','+lastName+',,,,,,,,,,,'+emailAddr+','+isEmployee+','+empActive+','+empCalendarCode+',,,'+isSub+','+subActive+','+availNewJobs+',,,,,,,,,,,,Y,|', file=outputfile)
									print('W,'+addOrChange+',2,1,'+accessID+','+emailAddr+',|', file=outputw2) #output the W2 SSO file
									if (amHalfTime and pmHalfTime and classificationCode and startTime and endTime): #only try to output to the E1 file if they have the 3 required pieces of data
										print('E,'+addOrChange+',1,'+accessID+',1,'+str(homeschool)+',1,'+classificationCode+','+startTime+','+endTime+',N,NYYYYYN,'+amHalfTime+','+pmHalfTime+',Y,|', file=outpute1) #output the E1 file
								else:
									print("Skipping contract employee " + firstName + " " + lastName)
					except Exception as er:
						print('Error on user: ' + str(entry[9]) + ': ' + str(er))
				inputDCIDFile.seek(0) #move the file pointer to the start of the file
				inputDCIDFile.truncate() #delete the current text
				inputDCIDFile.write(str(empDCID)) #write the last employee dcid as the newest one

			except Exception as er:
				print('High Level Error: '+str(er))


# the SFE import goes in order of oldest file to newest, so we manually set the ages of the files to be in the correct order
now = datetime.now().timestamp()
os.utime('P1ProfileBasic.csv', (now + 60,now+60))
os.utime('W2SSO.csv', (now + 120,now+120))
os.utime('E1PlusEmployeeWorkSchedule.csv', (now + 180,now+180))

#after all the files are done writing and now closed, open an sftp connection to the server and place the file on there
with pysftp.Connection(sftpHOST, username=sftpUN, private_key='private.pem', cnopts=cnopts) as sftp: # uses a private key file to authenticate with the server, need to pass the path
	print('SFTP connection established')
	# print(sftp.pwd) # debug, show what folder we connected to
	# print(sftp.listdir())  # debug, show what other files/folders are in the current directory
	sftp.chdir('./upload1')  # change to the extensionfields folder
	# print(sftp.pwd) # debug, make sure out changedir worked
	# print(sftp.listdir())
	sftp.put('P1ProfileBasic.csv')  # upload the first file onto the sftp server
	sftp.put('W2SSO.csv')  # upload the second file onto the sftp server
	sftp.put('E1PlusEmployeeWorkSchedule.csv') # upload final file
	print("Staff files placed on remote server")
	print("Staff files placed on remote server", file=outputLog)
outputLog.close() #close the log file
