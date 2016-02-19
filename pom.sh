#!/bin/bash

mvn -s C:\Users\butfly\.m2\settings-local.xml -f parent.pom deploy:deploy-file -DpomFile=parent.pom -Dfile=parent.pom -Durl=http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots/