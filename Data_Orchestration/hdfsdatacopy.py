#!/usr/bin/python
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from time import gmtime, strftime
import os
import socket

# VARIABLES MODIFIED ACCORDING TO ENVIRONMENT - CURRENT CUSTOMISED FOR PRODUCTION CLUSTER
queuename="default" # MAP-REDUCE QUEUE NAME

append="/tmp"	# TEMP DIRECTORY OF HDFS ON DEV/UAT CLUSTER
prodcluster="prodxyz.com:8020" # PRODUCTION HDFS NAMENODE RPC URL
nonprodcluster="devxyz.com:8020" # DEV/UAT HDFS NAMENODE RPC URL 
rownumber="" # FOR STORING CURRENT ROW THE SCRIPT IS ITERATING

prodhostname="prod.clientnode.com"
devhostname="dev.clientnode.com"
uathostname="uat.clientnode.com"




time=strftime("%Y-%m-%d-%H:%M:%S", gmtime())+"-" # TIME PREFIX TO UNIQUELY IDENTIFY EACH OPERATION

scope = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope) # OAuth2 GOOGLE CREDENTIALS TO CONNECT TO GOOGLE SHEET

gc = gspread.authorize(credentials)

wks = gc.open("test").sheet1 # OPENING GOOGLE SHEET

cell_list = wks.range('N2:N100')	# EXPLORING THE COPY STATUS - "N CLOUMN" 

postappend=""
preappend=""
sourcecluster=""
destcluster=""
fromfile = ""
tofile = ""
timeprefix=""
initialstatus=""

# FETCH THE SOURCE AND DESTINATION PATH OF ROWS WHOSE COPY STATUS IS READY AND MODIFY IT TO "In Progress" ALONG WITH TIMEPREFIX
for cell in cell_list: #ITERATE ROW BY ROW
        
	postappend=""
	preappend=""
	sourcecluster=""
	destcluster=""         
  	sourcehostname=""
  
  	initialstatus=cell.value

        if ( (cell.value  == "Ready" or cell.value[0:21] == "Starting Copy Request" or cell.value[0:23] == "Completing Copy Request") ):
        	timeprefix=str(cell.row)+"-"+str(time)
        
        if not cell.value:
                if not ( wks.acell("K"+str(cell.row)).value and wks.acell("H"+str(cell.row)).value and wks.acell("I"+str(cell.row)).value and wks.acell("J"+str(cell.row)).value ):
                        break   #EXIT FOR LOOP IF BLANK ROW COMES
                        
        ##############################################
        copyrequestover = os.system("ps -aef | grep '"+ str(cell.row) +"-201.*-copyscript.sh' | grep -v grep")			#0 if copy is over  and  #1 copy is over
        ##############################################
        

      	if ( (cell.value  == "Ready" or cell.value[0:21] == "Starting Copy Request" or cell.value[0:23] == "Completing Copy Request") and copyrequestover>0 ):
		if ( wks.acell("H"+str(cell.row)).value and wks.acell("I"+str(cell.row)).value and wks.acell("J"+str(cell.row)).value and wks.acell("K"+str(cell.row)).value ):		
			fromfile = open(timeprefix+"copyfrom.txt", "w")	# SOURCE PATH TO COPY
			tofile = open(timeprefix+"copyto.txt", "w")	# DESTINATION PATH TO COPY
		      
 
        if cell.value == "Ready":
		if ( wks.acell("H"+str(cell.row)).value and wks.acell("I"+str(cell.row)).value and wks.acell("J"+str(cell.row)).value and wks.acell("K"+str(cell.row)).value ):
              		fromfile.write(str(wks.acell("I"+str(cell.row)).value))
		        tofile.write(str(wks.acell("K"+str(cell.row)).value))
               		wks.update_acell("N"+str(cell.row),"Starting Copy Request - "+timeprefix)		

			postappend=append
			destcluster=nonprodcluster
			if wks.acell("J"+str(cell.row)).value == "Prod" and wks.acell("H"+str(cell.row)).value == "Prod" :
				destcluster=prodcluster
			if wks.acell("H"+str(cell.row)).value == "Prod" :      
				sourcecluster=prodcluster
			else :
				sourcecluster=nonprodcluster
											 
        if ( cell.value[0:21] == "Starting Copy Request" and copyrequestover>0 ):
    		if ( wks.acell("H"+str(cell.row)).value and wks.acell("I"+str(cell.row)).value and wks.acell("J"+str(cell.row)).value and wks.acell("K"+str(cell.row)).value ):
                        fromfile.write(str(wks.acell("I"+str(cell.row)).value))
                        tofile.write(str(wks.acell("K"+str(cell.row)).value))
                        wks.update_acell("N"+str(cell.row),"Completing Copy Request - "+timeprefix)
												
	         	preappend=append
                       	sourcecluster=nonprodcluster
                       	if (wks.acell("H"+str(cell.row)).value == "Prod" and wks.acell("J"+str(cell.row)).value == "Prod") :
	                	sourcecluster=prodcluster
			
	               	if wks.acell("J"+str(cell.row)).value == "Prod" :     
	                  	destcluster=prodcluster
                   	else :
                     		destcluster=nonprodcluster
			
			####### Put error log in the cell ######################
        if cell.value[0:23] == "Completing Copy Request" and copyrequestover:														  
		wks.update_acell("N"+str(cell.row),"Complete")

	######## Add more erro log to the cell###########

        if( sourcecluster == prodcluster ) :
           sourcehostname=prodhostname
        elif( sourcecluster == nonprodcluster and wks.acell( "H"+str(cell.row)).value == "Dev" ) :
           sourcehostname=devhostname
        elif( sourcecluster == nonprodcluster and wks.acell( "H"+str(cell.row)).value == "UAT" ) :
           sourcehostname=uathostname



	
	if ( (cell.value  == "Ready" or cell.value[0:21] == "Starting Copy Request" ) and copyrequestover>0 ):
	   if ( wks.acell("H"+str(cell.row)).value and wks.acell("I"+str(cell.row)).value and wks.acell("J"+str(cell.row)).value and wks.acell("K"+str(cell.row)).value ):							
		# CREATING FILE FOR COPY-SCRIPT TO EXECUTE DATA COPY FROM SOURCE PATH TO TEMPORARY PATH ON DESTINATION CLUSTER IN (/pzhdusr or /tmp) DIRECTORY ON DEV/UAT CLUSTER
		fromfile = open(timeprefix+"copyfrom.txt", "r")
		tofile = open(timeprefix+"copyto.txt", "r")
		putfile = open(timeprefix+"user.properties", "w")
		for line in fromfile:
       			putfile.write( line.rstrip() +"|"+ tofile.readline().strip()+"\n")


		# CREATE SHELL SCRIPT FOR CREATING HDFS TEMP DIRECTORY
		fromfile = open(timeprefix+"copyto.txt", "r")
		putfile = open(timeprefix+"createhdfsdirectory.sh", "w")
		for line in fromfile:
 		       putfile.write("hadoop fs -mkdir hdfs://"+destcluster+ postappend +line.rstrip()+"\n")


		# CREATE SHELL SCRIPT FOR THE DATA TO BE COPIED OVER HDFS TEMP DIRECTORY
		copyscript = open(timeprefix+"copyscript.sh", "w")
		script = """
		#!/bin/bash
		USER_COMMAND="${BASH_SOURCE[0]}"
		SCRIPT_HOME=`dirname $USER_COMMAND`
		. $SCRIPT_HOME/"""+timeprefix+"""user.properties
		IFS='|'
		while read source destination
		do
			hadoop distcp -Dmapreduce.job.queuename="""+queuename+""" -skipcrccheck -update  hdfs://"""+sourcecluster+ preappend +"""/$source hdfs://"""+ destcluster + postappend +"""/$destination
		done < "$SCRIPT_HOME/"""+timeprefix+"""user.properties"
		"""
		copyscript.write(script)


		# EXECUTE THE SHELL SCRIPT CREATED BY THE PYTHON SCRIPT ABOVE TO CREATE HDFS TEMP DIRECTORY AND COPY THE HDFS FILES TO DESTINATION PATH

		if(sourcehostname == socket.gethostname() and initialstatus  == "Ready"):
			os.system("sh "+timeprefix+"createhdfsdirectory.sh")
		if( (sourcehostname == socket.gethostname() and ( initialstatus  == "Ready" or initialstatus[0:21] == "Starting Copy Request"))):
			os.system("sh "+timeprefix+"copyscript.sh")
			
