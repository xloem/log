#sudo nice -n -5 $(type -p ffmpeg) -f x11grab -rtbufsize $((2**31-1)) -i :0 -f pulse -i default -c:v libx264 -preset ultrafast -crf 28 -c:a libopus -f matroska -movflags +faststart - | python3 capture_stdin.py
#nice -n 5 $(type -p ffmpeg) -f x11grab -rtbufsize $((2**31-1)) -i :0 -f pulse -i default -c:v libx264 -preset ultrafast -crf 28 -c:a libopus -f matroska -movflags +faststart - | python3 capture_stdin.py
sudo echo ''
{
    ffmpeg -f x11grab -rtbufsize $((2**31-1)) -i :0 -f pulse -i default -c:v libx264 -preset ultrafast -crf 28 -c:a libopus -f matroska -movflags +faststart - &
    sudo renice -n -5 --pid $! 1>&2
    wait 1>&2
}  | python3 capture_stdin.py
