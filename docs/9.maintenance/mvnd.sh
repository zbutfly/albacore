#!/bin/bash

mvn -P cominfo -Dmaven.props.skip=true -DaltDeploymentRepository=SSRD-bigdata-snapshot::default::http://af.hikvision.com.cn/artifactory/SSRD-bigdata-snapshot $* deploy

