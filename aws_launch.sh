#!/bin/bash
# LOCAL_SCHEDULER=

# while getopts I:O:R:C:S:t:s:w:l option
# do
# case "${option}"
# in
# I) INPUT_DIR=${OPTARG};;
# O) OUTPUT_DIR=${OPTARG};;
# R) RESOURCES_DIR=${OPTARG};;
# C) CODE_DIR=${OPTARG};;
# S) SEQ_PIPELINE_DIR=${OPTARG};;
# t) THREADS=${OPTARG};;
# s) SAMPLE_THREADS=$OPTARG;;
# w) WORKERS=$OPTARG;;
# l) LOCAL_SCHEDULER="-l";;
# esac
# done

INPUT_DIR=~/input
OUTPUT_DIR=~
RESOURCES_DIR=~/resources
CODE_DIR=~/code
SEQ_PIPELINE_DIR=~/seq_pipeline
THREADS=96
SAMPLE_THREADS=6
WORKERS=100
LOCAL_SCHEDULER=

cd ~/seq_pipeline
rm -rf luigi.cfg || true
git checkout luigi.cfg
git pull
python3.6 ./seq_pipeline/add_resources.py $THREADS
sudo luigid & &> ~/luigid_log.txt
# module load CBC python udocker
# udocker run --novol=/etc/host.conf --novol=/etc/resolv.conf -v /data/wpierson/seq_pipeline/resources:/root/pipeline/resources -v /data/wpierson/seq_pipeline/code:/root/pipeline/code -v ${INPUT_DIR}:/root/input -v ${OUTPUT_DIR}/output:/root/output -v /home/wpierson/projects/seq_pipeline:/root/seq_pipeline seq_pipeline /root/seq_pipeline/test_launch.sh -I /root/input -O /root -t $THREADS -s $SAMPLE_THREADS -w $WORKERS $LOCAL_SCHEDULER module load CBC python udocker
sudo docker run -v ${RESOURCES_DIR}:/root/pipeline/resources -v ${CODE_DIR}:/root/pipeline/code -v ${INPUT_DIR}:/root/input -v ${OUTPUT_DIR}/output:/root/output -v ${SEQ_PIPELINE_DIR}:/root/seq_pipeline wpierson/seq_pipeline:1.0 /root/seq_pipeline/aws_pipeline.sh -I /root/input -O /root -t $THREADS -s $SAMPLE_THREADS -w $WORKERS $LOCAL_SCHEDULER 