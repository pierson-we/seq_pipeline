. /root/.profile
cd /root
export LUIGI_CONFIG_PATH=/root/seq_pipeline/luigi.cfg && python3 ./seq_pipeline/run.py "$@"
