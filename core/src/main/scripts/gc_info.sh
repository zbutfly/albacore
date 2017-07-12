_GC_LOG="./~logs/gc${TSTR}.log"

JAVA_OPTS="${JAVA_OPTS} -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps \
-XX:+PrintGCApplicationStoppedTime -Xloggc:${_GC_LOG} \
-XX:+PrintStringDeduplicationStatistics \
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=."

echo GC Log File: ${_GC_LOG}

