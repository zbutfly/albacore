#!/bin/bash

mvn -f butfly.pom deploy:deploy-file -DpomFile=butfly.pom -Dfile=butfly.pom -Durl=http://repos.corp.butfly.co:60080/nexus/content/repositories/releases/

#mvn deploy:deploy-file -DpomFile=butfly.pom -Dfile=butfly.pom -Durl=http://repos.corp.butfly.co:60080/nexus/content/repositories/snapshots/
