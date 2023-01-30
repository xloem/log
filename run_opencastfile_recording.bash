sudo echo ''
find /proc/*/fd -type l 2>/dev/null | xargs readlink | grep \.cast$ | while read castfile
do
	tail -F -c +1 "$castfile" | {
		exec 4<&0 0</dev/tty
		python3 "$@" capture_stdin.py 4
	} &
done
wait
