#!/bin/bash

mvn -P cominfo -Dmaven.props.skip=true -DaltDeploymentRepository=nexus::default::http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots $* deploy

