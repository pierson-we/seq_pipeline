#!/bin/bash
. /root/.profile
apt-get -y install libapache-dbi-perl libtry-tiny-perl cmake git
cd /opt
wget https://github.com/samtools/samtools/releases/download/1.2/samtools-1.2.tar.bz2 && tar -xvjf samtools-1.2.tar.bz2 && rm -f samtools-1.2.tar.bz2
git clone https://github.com/genome/bam-readcount.git
cd /opt/bam-readcount && cmake -Wno-dev && make
cd /opt/samtools-1.2 && make -j && make install
export PATH=/opt/bam-readcount/bin:/opt/samtools-1.2/bin:$PATH
cd /root
alias python=/usr/bin/python3.6
alias python3=/usr/bin/python3.6
export LUIGI_CONFIG_PATH=/root/seq_pipeline/luigi.cfg
export LD_LIBRARY_PATH=/root/pipeline/code/source/htslib-1.9
# cat ./seq_pipeline/luigi.cfg
python3.6 ./seq_pipeline/run.py "$@"