#!/bin/bash
. /root/.profile
apt-get -y install libapache-dbi-perl libtry-tiny-perl cmake
cd /opt
wget https://github.com/genome/bam-readcount/archive/v0.7.4.tar.gz && tar xvzf v0.7.4.tar.gz && rm -f v0.7.4.tar.gz
wget https://github.com/samtools/samtools/releases/download/1.2/samtools-1.2.tar.bz2 && tar -xvjf samtools-1.2.tar.bz2 && rm -f samtools-1.2.tar.bz2
cd /opt/bam-readcount-0.7.4 && mkdir build && cd build && cmake ../ && make deps && make -j && make install
cd /opt/samtools-1.2 && make -j && make install
cd /root
alias python=/usr/bin/python3.6
alias python3=/usr/bin/python3.6
export LUIGI_CONFIG_PATH=/root/seq_pipeline/luigi.cfg
export LD_LIBRARY_PATH=/root/pipeline/code/source/htslib-1.9
# cat ./seq_pipeline/luigi.cfg
python3.6 ./seq_pipeline/run.py "$@"