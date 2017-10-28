from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from time import gmtime, strftime
from time import sleep
import socket
import subprocess
import os
import sys

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/gmail-python-quickstart.json
SCOPES = 'https://spreadsheets.google.com/feeds'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Gmail API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.
    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.
    Returns:
	Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
	os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
				   'gmail-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
	flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
	flow.user_agent = APPLICATION_NAME
	if flags:
	    credentials = tools.run_flow(flow, store, flags)
	else: # Needed only for compatibility with Python 2.6
	    credentials = tools.run(flow, store)
	print('Storing credentials to ' + credential_path)
    return credentials


def main():

	credentials = get_credentials()

	sourcecluster=""
	destcluster=""
	fromfile = ""
	tofile = ""
	timeprefix=""
	initialstatus=""
	rownumber="" # FOR STORING CURRENT ROW THE SCRIPT IS ITERATING
	chowncommand="" # For Dev to UAT to change rights of destination path from nzhdusr to nzhdhc1

	#scope = ['https://spreadsheets.google.com/feeds']
	#credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) # OAuth2 GOOGLE CREDENTIALS TO CONNECT TO GOOGLE SHEET

	gc = gspread.authorize(credentials)
	wks = gc.open("Release_DDL_Scripts").sheet1 # OPENING GOOGLE SHEET
	cell_list = wks.range('G2:G100')	   # EXPLORING THE COPY STATUS - "G CLOUMN"


	queuename="short_running" # MAP-REDUCE QUEUE NAME

	systemhostname = socket.gethostname()
	prodhostname="host123"
	devhostname="host234"
	uathostname="host456"
	uatusername="user"



	time=strftime("%Y-%m-%d-%H:%M:%S", gmtime())+"-" # TIME PREFIX TO UNIQUELY IDENTIFY EACH OPERATION
	linuxsystem = ""
	if ( systemhostname == prodhostname ) :
		linuxsystem = "Prod"
	elif ( systemhostname == devhostname ) :
		linuxsystem = "Dev"
	elif ( systemhostname == uathostname ) :
		linuxsystem = "UAT"

	print "Execution System : " + linuxsystem


	for cell in cell_list:

		postappend=""
		preappend=""
		sourcecluster=""
		destcluster=""
		sourcehostname=""
		initialstatus=cell.value
		# Resetting Phase of Current Row
		phase = 0
		datamissinginrow=1
		chowncommand="false"

		# Getting Source and Destination Clusters from the sheet
		#sheetsource = wks.acell("H"+str(cell.row)).value
		sheetdestination = "UAT"
		sourcehostname = "host456"
		print cell.row

		if ( wks.acell("E"+str(cell.row)).value and wks.acell("F"+str(cell.row)).value ) :
			datamissinginrow=0
		else :
			break

		# Deciding Phase of the Copy Request
		if ( initialstatus == "Ready" ) :
			phase = 1
		elif ( initialstatus[0:11] == "In Progress" ) :
			phase = 2

		print linuxsystem + "   " + sheetdestination

		if ( os.system("ps -aef | grep "+ str(cell.row) +"-201*-copyscript.sh | grep -v grep" ) == 0 ) :
			print "Copy for Row-" + str(cell.row) + " is Excuting"
			copyrequestover=0
		else :
			print "Copy for Row-" + str(cell.row) + " is Not Executing"
			copyrequestover=1


		print "Phase : " + str (phase)
		print "System Host : " + linuxsystem
		print "Source Hostname : " + sourcehostname

		if ( sourcehostname == systemhostname and phase>0 and copyrequestover > 0 ) :
			if ( datamissinginrow == 0 ) :
				timeprefix=str(cell.row)+"-"+str(time)
				#fromfile = open(timeprefix+"copyfrom.txt", "w") # SOURCE PATH TO COPY
				tofile = open(timeprefix+"commands.txt", "w")    # DESTINATION PATH TO COPY

				if ( phase == 1 ) :
					#fromfile.write(str(wks.acell("I"+str(cell.row)).value))
					tofile.write(str(wks.acell("F"+str(cell.row)).value))
					tofile.close()
					wks.update_acell("G"+str(cell.row),"In Progress")

					# CREATE SHELL SCRIPT FOR THE DATA TO BE COPIED OVER HDFS TEMP DIRECTORY
					copyscript = open(timeprefix+"copyscript.sh", "w")

					script = """#!/bin/bash
					USER_COMMAND="${BASH_SOURCE[0]}"
					SCRIPT_HOME=`dirname $USER_COMMAND`
					"""
					if ( str(wks.acell("E"+str(cell.row)).value) == "Linux" ) :
						script = script+"sh $SCRIPT_HOME/"+timeprefix+"commands.txt"
					elif ( str(wks.acell("E"+str(cell.row)).value) == "Hive" ) :
						script = script+"hive -f $SCRIPT_HOME/"+timeprefix+"commands.txt"
					elif ( str(wks.acell("E"+str(cell.row)).value) == "HBASE" ) :
						script = script+"hbase shell $SCRIPT_HOME/"+timeprefix+"commands.txt"

					copyscript.write(script)
					copyscript.close()

					# EXECUTE THE SHELL SCRIPT CREATED BY THE PYTHON SCRIPT ABOVE TO CREATE HDFS TEMP DIRECTORY AND COPY THE HDFS FILES TO DESTINATION PATH
					print "Executing Script"
					#os.system("cat "+timeprefix+"copyscript.sh; nohup sh "+timeprefix+"copyscript.sh >"+timeprefix+"copy.log")
					os.system("cat "+timeprefix+"commands.txt;" +"cat "+timeprefix+"copyscript.sh; ")
					os.system("sh "+timeprefix+"copyscript.sh -f -z -y > "+ timeprefix +"stdout.txt 2> "+ timeprefix +"stderr.txt")

					with open(timeprefix +"stderr.txt", 'r') as errfile:
						logfile=errfile.read()
					wks.update_acell("H"+str(cell.row),logfile)
					errfile.close()

					with open(timeprefix +"stdout.txt", 'r') as outfile:
						logfile=outfile.read()
					wks.update_acell("I"+str(cell.row),logfile)
					outfile.close()

				elif ( phase == 2 ) :
					wks.update_acell("G"+str(cell.row),"Done")

					mailscript = """mail -s 'DDL Hase Been Executed' """ + wks.acell("C"+str(cell.row)).value +"""<< EOF
Hi """ + wks.acell("D"+str(cell.row)).value + """
DDL Hase Been Executed
Type : """ + wks.acell("E"+str(cell.row)).value + """
Commands Executed :
"""+ wks.acell("F"+str(cell.row)).value + """
Output Logs :
"""+ wks.acell("J"+str(cell.row)).value + """
Error Logs :
"""+ wks.acell("H"+str(cell.row)).value + """

Thanks and Regards
EOF"""
					os.system(mailscript)

			else :
				break

if __name__ == '__main__':
    main()
