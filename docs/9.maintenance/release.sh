#!/bin/bash

mvn -P cominfo -DaltDeploymentRepository=nexus::default::http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/releases $* deploy
