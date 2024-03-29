LOCAL_SCHEDULER=

while getopts I:O:t:s:w:l option
do
case "${option}"
in
I) INPUT_DIR=${OPTARG};;
O) OUTPUT_DIR=${OPTARG};;
t) THREADS=${OPTARG};;
s) SAMPLE_THREADS=$OPTARG;;
w) WORKERS=$OPTARG;;
l) LOCAL_SCHEDULER="-l";;
esac
done

module load CBC python udocker
udocker run --novol=/etc/host.conf --novol=/etc/resolv.conf -v /data/wpierson/seq_pipeline/resources:/root/pipeline/resources -v /data/wpierson/seq_pipeline/code:/root/pipeline/code -v ${INPUT_DIR}:/root/input -v ${OUTPUT_DIR}/output:/root/output -v /home/wpierson/projects/seq_pipeline:/root/seq_pipeline seq_pipeline /root/seq_pipeline/test_launch.sh -C -I /root/input -O /root -t $THREADS -s $SAMPLE_THREADS -w $WORKERS $LOCAL_SCHEDULER module load CBC python udocker
# docker run -v ${RESOURCES_DIR}:/root/pipeline/resources -v ${CODE_DIR}:/root/pipeline/code -v ${INPUT_DIR}:/root/input -v ${OUTPUT_DIR}/output:/root/output -v ${SEQ_PIPELINE_DIR}:/root/seq_pipeline seq_pipeline /root/seq_pipeline/test_launch.sh -I /root/input -O /root -t $THREADS -s $SAMPLE_THREADS -w $WORKERS $LOCAL_SCHEDULER 