spark-submit master.py > log.txt 2> logstream.txt
echo "FINISHED STREAMING, NOW COMPUTING METRICS"
python3 metrics.py
echo "RUNNING UI"
spark-submit ui.py $1 2> logui.txt