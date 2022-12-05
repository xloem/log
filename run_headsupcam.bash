if [ -z "$DISPLAY" ]
then
    DISPLAY=:0
fi
export DISPLAY
while true
do
    ffplay -f v4l2 -video_size 320x200  /dev/video1
done

