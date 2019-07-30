#!/bin/bash
. /root/.profile
cd /root
apt-get -y install libapache-dbi-perl libtry-tiny-perl
alias python=/usr/bin/python3.6
alias python3=/usr/bin/python3.6
export LUIGI_CONFIG_PATH=/root/seq_pipeline/luigi.cfg
# cat ./seq_pipeline/luigi.cfg
python3.6 ./seq_pipeline/run.py "$@"