#!/usr/bin/env bash
PROXYCMD=oops
for MAX_TIME in 1 2 5 10
do
    TESTCMD="curl --max-time $MAX_TIME -sIf google.com"
    if $TESTCMD > /dev/null
    then
        PROXYCMD=
        break
    elif proxychains4 $TESTCMD > /dev/null
    then
        PROXYCMD=proxychains4
        break
    fi
    echo 'Connection troubles ....'
done
if [ "$PROXYCMD" == "oops" ]
then
    exit -1
fi
export FN="$(date --iso=seconds)".cast
export PROXYCMD

mkfifo "$FN".fifo
asciinema rec --stdin "$FN".fifo -e FN,PROXYCMD,SHELL,TERM -c "$(dirname "$0")/run_asciinema_internal.bash"
rm "$FN".fifo
