import subprocess
import luigi
import os
import sys
import pipeline_utils
import preprocess

class mutect2_normal(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'preprocess': preprocess(case=self.case, sample=self.sample, cfg=self.cfg)}

	def output(self):
		return {'mutect2_normal': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_%s_mutect2_normal.vcf.gz' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_mutect2_normal_err.txt' % (self.case, self.sample)))}

	def run(self):
		cmd = ['gatk4', 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '-I', self.input()['preprocess'].path, '-O', self.output()['mutect2_normal'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class mutect2_pon(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {}
		for case in self.cfg['cases']:
			if 'N' in self.cfg['cases'][case]:
				requirements[case] = {'mutect2_normal': mutect2_normal(case=case, sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		return {'mutect2_pon': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'variant_prep', 'mutect2_pon.vcf.gz')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'mutect2_pon_err.txt'))}

	def run(self):
		cmd = ['gatk4', 'CreateSomaticPanelOfNormals', '-R', self.cfg['fasta_file'], '-O', self.output()['mutect2_pon'].path]
		for case in self.input():
			cmd += ['-vcfs', self.input()[case]['mutect2_normal']['mutect2_normal'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class mutect2(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		if 
		return {'preprocess': preprocess(case=self.case, sample=self.sample, cfg=self.cfg), 'mutect2_pon': mutect2_pon(cfg=self.cfg)}

	def output(self):
		return {'mutect2': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_mutect2.vcf.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_mutect2_err.txt' % self.case))}

	def run(self):
		cmd = ['gatk4', 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '-I', self.input()['preprocess'].path, '-O', self.output()['mutect2_normal'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)


