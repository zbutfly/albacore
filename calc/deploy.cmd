call mvn deploy:deploy-file -f calculus-deps.pom -DpomFile=calculus-deps.pom -Dfile=calculus-deps.pom -DrepositoryId=nexus -Durl=http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots
call mvn deploy:deploy-file -f calculus-parent.pom -DpomFile=calculus-parent.pom -Dfile=calculus-parent.pom -DrepositoryId=nexus -Durl=http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots
call mvn deploy -DaltDeploymentRepository=nexus::default::http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots

