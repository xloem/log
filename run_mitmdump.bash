
mitmdump "$@" -w /dev/stderr 3>&1 1>&2 2>&3 | python3 capture_stdin.py
