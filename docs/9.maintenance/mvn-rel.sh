#!/bin/bash

mvn -P cominfo -DaltDeploymentRepository=SSRD-bigdata-release::default::http://af.hikvision.com.cn/artifactory/SSRD-bigdata-release $* deploy
