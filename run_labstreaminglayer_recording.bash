sudo echo ''
{
    LabRecorderCLI /dev/fd/3 '' 3>&1 1>&2
    sudo renice -n -5 --pid $! 1>&2
    wait 1>&2
} | {
    exec 4<&0 0</dev/tty
    python3 "$@" capture_stdin.py 4
}
