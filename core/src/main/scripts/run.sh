#!/bin/bash
set -o nounset
set -o errexit
export LANG=zh_CN.UTF-8

export JAVA_HOME=" /usr/java/jdk1.8.0_60/"
export PATH=${JAVA_HOME}/bin:${PATH}
java -version

TSTR=`date +"%Y%m%d%H%M%s"`
if [ -z ${JAVA_OPTS+x} ]; then JAVA_OPTS=; else echo "Original JAVA_OPTS: ${JAVA_OPTS}"; fi
JAVA_OPTS="${JAVA_OPTS} -d64 -server"
#JAVA_OPTS="${JAVA_OPTS} -DDEBUG.MONGO=true -DDB.TRACE=true"
#. ./gc_info.sh
. ./gc_cms.sh
#JAVA_OPTS="${JAVA_OPTS} -Xms1g -Xmx32g -Xss4m"
#JAVA_OPTS="${JAVA_OPTS} -Djava.util.concurrent.ForkJoinPool.common.parallelism=16"
#JAVA_OPTS="${JAVA_OPTS} -agentpath:/opt/jprofiler/bin/linux-x64/libjprofilerti.so=port=18999"
#JAVA_OPTS="${JAVA_OPTS} -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=18089"
#JAVA_OPTS="${JAVA_OPTS} -Dalbacore.gc.interval.ms=3000"
#JAVA_OPTS="${JAVA_OPTS} -Dalbacore.parallel.factor=2"

# TODO: Application configuration processing...

echo JAVA_OPTS:			${JAVA_OPTS}
#_CURR_JAR=
_CURR_CLSP="./test-classes:./${_CURR_JAR}.jar:./dependency/*"
echo Java Classpath:	${_CURR_CLSP}

# TODO: Application command line args processing... 

echo Command Line Args: $*
#_CURR_MAIN=
_CURR_CMDL="java ${JAVA_OPTS} -cp ${_CURR_CLSP} ${CURR_MAIN}"
echo Command to be Run: ${_CURR_CMDL}
${_CURR_CMDL}
