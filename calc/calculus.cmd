call mvn -f calculus-deps.pom deploy:deploy-file -DpomFile=calculus-deps.pom -Dfile=calculus-deps.pom -DrepositoryId=nexus -Durl=http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots
call mvn -f calculus-deps.pom install:install-file -DpomFile=calculus-deps.pom -Dfile=calculus-deps.pom

REM call mvn -f calculus-parent.pom deploy:deploy-file -DpomFile=calculus-parent.pom -Dfile=calculus-parent.pom -DrepositoryId=nexus -Durl=http://repos.corp.hzcominfo.com:6080/nexus/content/repositories/snapshots
REM call mvn -f calculus-parent.pom install:install-file -DpomFile=calculus-parent.pom -Dfile=calculus-parent.pom
