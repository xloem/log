fn="$(date --iso=seconds)".m4a
on_trap() {
	termux-microphone-record -q
	kill "$(<"$fn".pid)"
	rm "$fn".pid
}
termux-microphone-record -f "$fn" -l 0
trap on_trap INT
trap on_trap TERM
{
	tail -c+0 -F "$fn" &
	echo $! > "$fn".pid
	wait
} | python3 capture_stdin.py &
wait
