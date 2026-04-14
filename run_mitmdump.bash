
mitmdump "$@" -w /dev/stderr 3>&1 1>&2 2>&3 | zstd --long --adapt --ultra | python3 capture_stdin.py
