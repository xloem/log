if [ -z "$DISPLAY" ]
then
    DISPLAY=:0
fi
export DISPLAY
while true
do
    ffplay -f v4l2 -video_size 320x200  "$(ls /dev/video* | tail -n 1)"
done

