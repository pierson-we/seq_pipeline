module load CBC
module load udocker

# udocker load -i /data/wpierson/seq_pipeline/pipeline_test_1.1.tar
# udocker create --name=seq_pipeline wpierson/pipeline_test:1.1
udocker run --nosysdirs -v /data/wpierson/seq_pipeline/resources:/root/pipeline/resources -v /data/wpierson/seq_pipeline/code:/root/pipeline/code -v /data/wpierson/endometrioid:/root/input -v /data/wpierson/endometrioid_2019:/root/output -v /home/wpierson/projects/seq_pipline:/root/seq_pipeline seq_pipeline /root/pipeline/code/test_launch.sh "$@"