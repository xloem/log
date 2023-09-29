#!/usr/bin/env bash

export FN PROXYCMD

# Check if script was started in our screen session
if [ -z "$INTERNAL_INIT_SCRIPT" ]; then
  # Create temporary screen config file to avoid conflicts with
  # user's .screenrc
  screencfg=$(mktemp)
  # Show status line at bottom of terminal
  echo hardstatus alwayslastline > "$screencfg"
  # Start script in a new screen session
  INTERNAL_INIT_SCRIPT=1 screen -mq -c "$screencfg" bash -c "$0"
  # Store screen return code
  ret=$?
  # Remove temporary screen config file
  rm "$screencfg"
  # Exit with the same return code that screen exits with
  exit $ret
fi

function set_status {
  screen -X hardstatus string "$*"
}

cat "$FN".fifo | tee "$FN" | $PROXYCMD python3 "$@" capture_stdin.py 2>&1 | tr '\r' '\n' | while read line
do
    set_status "$line"
done &
BGPID=$!
/usr/bin/env bash
kill $BGPID
