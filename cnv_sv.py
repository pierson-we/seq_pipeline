import subprocess
import luigi
import os
import sys
import pipeline_utils

class nbic_seq(luigi.Task):
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
    def resources(self):
        return {'threads': self.cfg['max_threads']}

	def requires(self):
		return

	def output(self):
		return 

	def run(self):
		cmd = 

class crest(luigi.Task):
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
    def resources(self):
        return {'threads': self.cfg['max_threads']}

	def requires(self):
		return

	def output(self):
		return 

	def run(self):
		cmd = 

class delly(luigi.Task):
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
    def resources(self):
        return {'threads': self.cfg['max_threads']}

	def requires(self):
		return

	def output(self):
		return 

	def run(self):
		cmd = 

class breakdancer(luigi.Task):
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
    def resources(self):
        return {'threads': self.cfg['max_threads']}

	def requires(self):
		return

	def output(self):
		return 

	def run(self):
		cmd = 

class cnvkit(luigi.Task):
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
    def resources(self):
        return {'threads': self.cfg['max_threads']}

	def requires(self):
		return

	def output(self):
		return 

	def run(self):
		cmd = 

