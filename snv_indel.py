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
		return {'preprocess': preprocess.preprocess(case=self.case, sample=self.sample, cfg=self.cfg)}

	def output(self):
		outputs =  {'mutect2_normal': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_%s_mutect2_normal.vcf.gz' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_mutect2_normal_err.txt' % (self.case, self.sample)))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['gatk4', 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '-I', self.input()['preprocess']['bam'].path, '-O', self.output()['mutect2_normal'].path]
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
		outputs = {'mutect2_pon': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'variant_prep', 'mutect2_pon.vcf.gz')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'mutect2_pon_err.txt'))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs


	def run(self):
		cmd = ['gatk4', 'CreateSomaticPanelOfNormals', '-R', self.cfg['fasta_file'], '-O', self.output()['mutect2_pon'].path]
		for case in self.input():
			cmd += ['-vcfs', self.input()[case]['mutect2_normal']['mutect2_normal'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class mutect2(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {'mutect2_pon': mutect2_pon(cfg=self.cfg)}
		requirements['T'] = {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}
		if 'N' in self.cfg['cases'][self.case]:
			requirements['N'] = {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		outputs = {'mutect2': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_mutect2.vcf.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_mutect2_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['gatk4', 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '--germline-resource', self.cfg['gnomad'], '--panel-of-normals', self.input()['mutect2_pon']['mutect2_pon'].path, '-I', self.input()['T']['preprocess']['bam'].path, '-O', self.output()['mutect2'].path]
		if 'N' in self.cfg['cases'][self.case]:
			cmd += ['-I', self.input()['T']['preprocess']['bam'].path, '-normal', '%s_N' % self.case]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class lofreq(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}}
		if 'N' in self.cfg['cases'][self.case]:
			requirements['N'] = {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		outputs = {'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_lofreq_err.txt' % self.case))}
		if 'N' in self.cfg['cases'][self.case]:
			outputs['lofreq'] = [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_lofreq_somatic_final_minus-dbsnp.%s.vcf.gz' % (self.case, variant_type))) for variant_type in ['snvs', 'indels']]
		else:
			outputs['lofreq'] = [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_lofreq.vcf.gz' % self.case))]
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
			elif isinstance(outputs[task], list):
				for item in outputs[task]:
					if isinstance(item, luigi.LocalTarget):
						pipeline_utils.confirm_path(item.path)
		return outputs

	def run(self):
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['lofreq', 'call-parallel', '-f', self.cfg['fasta_file'], '--call-indels', '--threads', self.cfg['max_threads'], '-l', self.cfg['library_bed'], '-d', self.cfg['germline_all'], '-o', self.output()['lofreq'][0].path.split('somatic_final_minus-dbsnp')[0], '-n', self.input()['N']['preprocess']['bam'].path, '-t', self.input()['T']['preprocess']['bam'].path]
		else:
			cmd = ['lofreq', 'somatic', '-f', self.cfg['fasta_file'], '--call-indels', '--pp-threads', self.cfg['max_threads'], '-l', self.cfg['library_bed'], '-s', '-S', self.cfg['germline_all'], '-o', self.output()['lofreq'][0].path, self.input()['T']['preprocess']['bam'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class manta(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}, 'N': {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}}

	def output(self):
		return {'manta': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta', 'results', 'variants', 'candidateSmallIndels.vcf.gz')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_manta_err.txt' % self.case))}

	def run(self):
		cmd = ['$MANTA/bin/configManta.py', '--exome', '--referenceFasta', self.cfg['fasta_file'], '--normalBam', self.input()['N']['preprocess']['bam'].path, '--tumorBam', self.input()['T']['preprocess']['bam'].path, '--rundir', os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta')] # TODO add logic for exome to handle multiple sequencing preps
		pipeline_utils.command_call(cmd)
		cmd = [os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta', 'runWorkflow.py'), '-j', self.max_threads]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class strelka(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'manta': manta(case=self.case, cfg=self.cfg), 'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}, 'N': {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}}

	def output(self):
		return {'strelka': [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka', 'results', 'variants', 'somatic.%s.vcf.gz' % variant_type)) for variant_type in ['snvs', 'indels']], 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_manta_err.txt' % self.case))}

	def run(self):
		cmd = ['$STRELKA/bin/configureStrelkaSomaticWorkflow.py', '--exome', '--referenceFasta', self.cfg['fasta_file'], '--normalBam', self.input()['N']['preprocess']['bam'].path, '--tumorBam', self.input()['T']['preprocess']['bam'].path, '--indelCandidates', self.input()['manta']['manta'].path, '--rundir', os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka')]
		pipeline_utils.command_call(cmd)
		cmd = [os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka', 'runWorkflow.py'), '-m', 'local', '-j', self.cfg['max_threads']]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class scalpel_discovery(luigi.Task):
	priority = 80
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}}
		if 'N' in self.cfg['cases'][self.case]:
			requirements['N'] = {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		outputs = {'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_lofreq_err.txt' % self.case))}
		if 'N' in self.cfg['cases'][self.case]:
			outputs['scalpel_discovery'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'twopass', 'database.db'))
		else:
			outputs['scalpel_discovery'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'database.db'))
		return outputs

	def run(self):
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['scalpel-discovery', '--somatic', '--two-pass', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path.split('twopass')[0]), '--format', 'vcf', '--normal', self.input()['N']['preprocess']['bam'].path, '--tumor', self.input()['T']['preprocess']['bam'].path]
		else:
			cmd = ['scalpel-discovery', '--single', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path), '--format', 'vcf', '--bam', self.input()['T']['preprocess']['bam'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class scalpel_export(luigi.Task):
	priority = 80
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	def requires(self):
		return {'scalpel_discovery': scalpel_discovery(case=self.case, cfg=self.cfg)}

	def output(self):
		outputs = {'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_lofreq_err.txt' % self.case))}
		if 'N' in self.cfg['cases'][self.case]:
			outputs['scalpel_export'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'twopass', 'somatic.indel.vcf'))
		else:
			outputs['scalpel_export'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'single.indel.vcf'))
		return outputs

	def run(self):
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['scalpel-export', '--somatic', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'],'-db', self.input()['scalpel_discovery']['scalpel_discovery'].path, '--output-format', 'vcf']
		else:
			cmd = ['scalpel-export', '--single', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'],'-db', self.input()['scalpel_discovery']['scalpel_discovery'].path, '--output-format', 'vcf']
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class variant_calling(luigi.Task):
	priority = 80
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	def requires(self):
		# requirements = {'scalpel_export': scalpel_export(case=self.case, cfg=self.cfg),
		# 'lofreq': lofreq(case=self.case, cfg=self.cfg),
		requirements = {'mutect2': mutect2(case=self.case, cfg=self.cfg)}
		# if 'N' in self.cfg['cases'][self.case]:
		# 	requirements['strelka'] = strelka(case=self.case, cfg=self.cfg)
		return requirements

	def output(self):
		return self.input()

# TODO filter out all germline variants I can find.
