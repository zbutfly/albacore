call mvn -f calculus-deps.pom install:install-file -DpomFile=calculus-deps.pom -Dfile=calculus-deps.pom
call mvn -f calculus-parent.pom install:install-file -DpomFile=calculus-parent.pom -Dfile=calculus-parent.pom

call mvn clean compile package install
