echo "export MANTA=/root/pipeline/code/source/manta-1.6.0" >> /root/.profile
echo "export STRELKA=/root/pipeline/code/source/strelka-2.9.10" >> /root/.profile
rm /usr/bin/java
update-alternatives --config java
. /root/.profile
cd /root
alias python=/usr/bin/python3.6
alias python3=/usr/bin/python3.6
export LUIGI_CONFIG_PATH=/root/seq_pipeline/luigi.cfg && python3.6 ./seq_pipeline/run.py "$@"
