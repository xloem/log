ffmpeg -f x11grab -rtbufsize $((2**31-1)) -i :0 -f pulse -i default -c:v libx264 -preset ultrafast -crf 28 -c:a libopus -f matroska -movflags +faststart - | python3 capture_stdin.py
