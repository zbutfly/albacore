#!/bin/bash

mvn -f parent.pom deploy:deploy-file -DpomFile=parent.pom -Dfile=parent.pom -Durl=http://repos.corp.butfly.co:60080/nexus/content/repositories/releases/
