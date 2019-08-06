import subprocess
import luigi
import os
import sys
import pipeline_utils
import preprocess

class facets_snp_pileup(luigi.Task):
	priority = 88
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	# @property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	# def resources(self):
	# 	return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}}
		if 'N' in self.cfg['cases'][self.case]:
			requirements['N'] = {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}
		else:
			requirements['N'] = {'preprocess': preprocess.preprocess(case='1010', sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		outputs = {'facets_snp_pileup': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_facets_snp_pileup.csv.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_facets_snp_pileup_err.txt' % self.case))}	
		if isinstance(outputs[task], luigi.LocalTarget):
			pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['snp-pileup', '-g', '-q15', '-Q20', '-P100', '-r25,0', self.cfg['germline_all'], self.output()['facets_snp_pileup'], self.input()['N']['preprocess']['bam'], self.input()['T']['preprocess']['bam']]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=2, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

# class facets(luigi.Task):
# 	priority = 88
# 	cfg = luigi.DictParameter()

# 	case = luigi.Parameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		requirements = {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}}
# 		if 'N' in self.cfg['cases'][self.case]:
# 			requirements['N'] = {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}
# 		return requirements

# 	def output(self):
# 		outputs = {'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_lofreq_err.txt' % self.case))}
# 		if 'N' in self.cfg['cases'][self.case]:
# 			outputs['scalpel_discovery'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'twopass', 'database.db'))
# 		else:
# 			outputs['scalpel_discovery'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'database.db'))
# 		for task in outputs:
# 			if isinstance(outputs[task], luigi.LocalTarget):
# 				pipeline_utils.confirm_path(outputs[task].path)
# 		return outputs

# 	def run(self):
# 		if 'N' in self.cfg['cases'][self.case]:
# 			cmd = ['scalpel-discovery', '--somatic', '--two-pass', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path.split('twopass')[0]), '--format', 'vcf', '--normal', self.input()['N']['preprocess']['bam'].path, '--tumor', self.input()['T']['preprocess']['bam'].path]
# 		else:
# 			cmd = ['scalpel-discovery', '--single', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path), '--format', 'vcf', '--bam', self.input()['T']['preprocess']['bam'].path]
# 		if self.cfg['cluster_exec']:
# 			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
# 		else:
# 			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

# class nbic_seq(luigi.Task):
# 	cfg = luigi.DictParameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		return

# 	def output(self):
# 		return 

# 	def run(self):
# 		cmd = 

# class crest(luigi.Task):
# 	cfg = luigi.DictParameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		return

# 	def output(self):
# 		return 

# 	def run(self):
# 		cmd = 

# class delly(luigi.Task):
# 	cfg = luigi.DictParameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		return

# 	def output(self):
# 		return 

# 	def run(self):
# 		cmd = 

# class breakdancer(luigi.Task):
# 	cfg = luigi.DictParameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		return

# 	def output(self):
# 		return 

# 	def run(self):
# 		cmd = 

# class cnvkit(luigi.Task):
# 	cfg = luigi.DictParameter()

# 	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
# 	def resources(self):
# 		return {'threads': self.cfg['max_threads']}

# 	def requires(self):
# 		return

# 	def output(self):
# 		return 

# 	def run(self):
# 		cmd = 

