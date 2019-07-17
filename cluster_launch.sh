module load CBC
module load python
module load udocker

# udocker load -i /data/wpierson/seq_pipeline/pipeline_test_1.1.tar
# udocker create --name=seq_pipeline wpierson/pipeline_test:1.1
udocker run --novol=/etc/host.conf -v /data/wpierson/seq_pipeline/resources:/root/pipeline/resources -v /data/wpierson/seq_pipeline/code:/root/pipeline/code -v /data/wpierson/endometrioid_small/input:/root/input -v /data/wpierson/endometrioid_small/output:/root/output -v /home/wpierson/projects/seq_pipeline:/root/seq_pipeline seq_pipeline /root/seq_pipeline/test_launch.sh "$@"