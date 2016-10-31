#!/bin/bash


usrname="username"
pass="password"
url="https://azkabanhostname:8443"

mail_id="shahrukhkhan489@gmail.com"

# Deploying Artifact on Linux


if [ -f /tmp/Deploy/*.tar ];
then
        rm -rf /azkaban/artifacts/$project//*
        mkdir -p /azkaban/artifacts/$project//libs
        mv /tmp/Deploy/*.tar /azkaban/artifacts/$project//libs/
        mail -s '$project tar has been copied' -c $mail_id << EOF
Hi Team

Tar Has been Copied.

Output of Folder : /azkaban/artifacts/$project//libs
`ll /azkaban/artifacts/$project/libs`

Thanks and Regards
Shahrukh
EOF

fi


# Deploying Artifact on Azkaban

if [ -f /tmp/Deploy/*.zip ];
then



curl --silent -k -X POST --data "action=login&username=$usrname&password=$pass" $url/ |tee /tmp/sid;session_id=`cat /tmp/sid |grep session.id|awk -F'"' '{print $4}'`


        for i in `ls /tmp/Deploy/*.zip`
            do
                        artifact_name='$project'
                    echo $(tput bold)$(tput setab 4)"$artifact_name"$(tput sgr0)

                                project_name=$artifact_name
                                project_flow="load_setup_flow"

                                echo $project_name
                                echo $project_flow

                                > /tmp/$projectlogs.txt

                                ZIP_PATH="$(echo $i)"

                                RESULTS="$(curl -s -k -i -H "Content-Type: multipart/mixed" -X POST --form "session.id=$session_id" --form 'ajax=upload' --form 'file=@'"$ZIP_PATH"';type=application/zip' --form "project=$project_name" $url/manager | grep "error" | wc -l)"

                                echo "Status of upload".$RESULTS;

                                if [[ $RESULTS != "0" ]]; then
                                        echo $(tput setaf 1)"#####Error uploading azkaban zip file for $project_name##########"$(tput sgr0) >> /tmp/$projectlogs.txt
                                else
                                        echo $(tput setaf 2)"#####Successfully uploaded azkaban file for $project_name#######"$(tput sgr0) >> /tmp/$projectlogs.txt
                                        if [ "$project_flow" != "" ]; then
                                for flow_name in $project_flow
                            do
                                                        JOB_RESULTS="$(curl -s -k --get --data "session.id=$session_id" --data 'ajax=executeFlow' --data "project=$project_name" --data "flow=$flow_name" $url/executor)"
                                                        if [[ $(echo "$JOB_RESULTS"| grep "execid" | wc -l) != "1" ]]; then
                                                                echo $(tput setaf 1)"#####Error submitted azkaban job flow $flow_name for $project_name##########"$(tput sgr0) >> /tmp/$projectlogs.txt
                                                        else
                                                                echo $(tput setaf 2)"#####Successfully submitted azkaban job flow $flow_name for $project_name########"$(tput sgr0) >> /tmp/$projectlogs.txt
                                                                JOB_RESULTS_STATUS=$(echo "$JOB_RESULTS" | grep "execid" |awk -F' ' '{print $3}')
                                        sleep 10
                                                                JOB_STATUS="$(curl -s -k --get --data "session.id=$session_id" --data 'ajax=fetchexecflow' --data "execid=$JOB_RESULTS_STATUS" $url/executor | grep "status" |awk -F'"' '{print $4}')"
                                                                for status in $JOB_STATUS
                                                                do
                                                                        if [ "$status" == "FAILED" ] || [ "$status" == "CANCELLED" ]; then
                                                                                echo $(tput setaf 1)"#####Error executing azkaban job flow $flow_name for $project_name##########"$(tput sgr0) >> /tmp/$projectlogs.txt
                                                                                break
                                                                        fi
                                                                done
                                                                echo $(tput setaf 2)"#####Successfully executed azkaban job flow $flow_name for $project_name########"$(tput sgr0) >> /tmp/$projectlogs.txt
                                                        fi
                            done
                                        else
                                                echo "No job defined for $project_name"
                                                continue
                                        fi
                                fi
                done

logs=`cat /tmp/$projectlogs.txt`

        mail -s '$project Zip has been uploaded and $project_flow has been executed' $mail_id << EOF
Hi Team

$project Zip has been uploaded to Azkaban and $project_flow has been executed

$logs

Thanks and Regards
Shahrukh
EOF


rm -rf /tmp/Deploy/*.zip

fi
