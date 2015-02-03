## for release
mvn deploy:deploy-file -DpomFile=butfly.pom -Dfile=butfly.pom -Durl=http://repos.corp.butfly.co:60080/nexus/content/repositories/releases/
## for snapshot
mvn deploy:deploy-file -DpomFile=butfly.pom -Dfile=butfly.pom -Durl=http://repos.corp.butfly.co:60080/nexus/content/repositories/snapshots/
