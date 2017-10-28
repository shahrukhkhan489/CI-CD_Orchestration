# HDFS Data Orchestartion

Big Data Orchestration Automation Workflows on Hadoop HDFS Clusters using 
- Python Scripting 
- Google Developers Console 
- OAuth 2.0 Authorization 
- HDFS Filesystem 
- DistCP Utlility 
- Shell Scripting

Requires "gspread" Python Library to be installed
# pip install gspread

Requires Google Sheet OAuth 2.0 API JSON Credential file to Access the Google Sheet which contains the source and destination path and cluster on coloumns  H, I, J and K conistuting of Source Cluster, Source HDFS Path, Destination Cluster, and Destination HDFS Path respectively.

Coloumn N contains the status of the copy request which is update by the script as the execution flows
