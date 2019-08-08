import subprocess
import luigi
import os
import sys
import shutil
import gzip
import pipeline_utils
import preprocess

class mutect2_normal(luigi.Task):
	priority = 89
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
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '-I', self.input()['preprocess']['bam'].path, '-O', self.output()['mutect2_normal'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

# class pon_genomicsDB(luigi.Task):
# 	priority = 88
# 	resources = {'threads': 1}
# 	cfg = luigi.DictParameter()

# 	def requires(self):
# 		requirements = {}
# 		for case in self.cfg['cases']:
# 			if 'N' in self.cfg['cases'][case]:
# 				requirements[case] = {'mutect2_normal': mutect2_normal(case=case, sample='N', cfg=self.cfg)}
# 		return requirements

# 	def output(self):
# 		outputs = {'pon_genomicsDB': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'variant_prep', 'pon_db', 'vcfheader.vcf')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'pon_genomicsDBerr.txt'))}
# 		for task in outputs:
# 			if isinstance(outputs[task], luigi.LocalTarget):
# 				pipeline_utils.confirm_path(outputs[task].path)
# 		return outputs


# 	def run(self):
# 		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'GenomicsDBImport', '-R', self.cfg['fasta_file'], '-L', self.cfg['library_bed'], '--merge-input-intervals', '--genomicsdb-workspace-path', os.path.dirname(self.output()['pon_genomicsDB'].path)]
# 		for case in self.input():
# 			cmd += ['-V', self.input()[case]['mutect2_normal']['mutect2_normal'].path]
# 		if self.cfg['cluster_exec']:
# 			pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
# 		else:
# 			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

# class mutect2_pon(luigi.Task):
# 	priority = 88
# 	resources = {'threads': 1}
# 	cfg = luigi.DictParameter()

# 	def requires(self):
# 		return {'pon_genomicsDB': pon_genomicsDB(cfg=self.cfg)}

# 	def output(self):
# 		outputs = {'mutect2_pon': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'variant_prep', 'mutect2_pon.vcf.gz')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'mutect2_pon_err.txt'))}
# 		for task in outputs:
# 			if isinstance(outputs[task], luigi.LocalTarget):
# 				pipeline_utils.confirm_path(outputs[task].path)
# 		return outputs


# 	def run(self):
# 		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'CreateSomaticPanelOfNormals', '-R', self.cfg['fasta_file'], '-V', 'gendb://%s' % os.path.dirname(self.input()['pon_genomicsDB']['pon_genomicsDB'].path), '-L', self.cfg['library_bed'], '-O', self.output()['mutect2_pon'].path]
# 		if self.cfg['cluster_exec']:
# 			pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
# 		else:
# 			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class mutect2_pon(luigi.Task):
	priority = 88
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

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
		cmd = ['java', '-Dsamjdk.use_async_io_read_samtools=false', '-Dsamjdk.use_async_io_write_samtools=true', '-Dsamjdk.use_async_io_write_tribble=false', '-Dsamjdk.compression_level=2', '-Djava.io.tmpdir=%s' % self.cfg['tmp_dir'], '-jar', '/root/pipeline/resources/broad/gatk-4.0.12.0/gatk-package-4.0.12.0-local.jar', 'CreateSomaticPanelOfNormals', '-O', self.output()['mutect2_pon'].path, '--tmp-dir', self.cfg['tmp_dir']]
		for case in self.input():
			cmd += ['-vcfs', self.input()[case]['mutect2_normal']['mutect2_normal'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class haplotype_caller(luigi.Task):
	priority = 89
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}

	def output(self):
		outputs =  {'haplotype_caller': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_haplotype_caller.vcf.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_haplotype_caller_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'HaplotypeCaller', '-R', self.cfg['fasta_file'], '-L', self.cfg['germline_genes'], '--native-pair-hmm-threads', self.cfg['max_threads'], '-I', self.input()['preprocess']['bam'].path, '-O', self.output()['haplotype_caller'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class filter_germline(luigi.Task):
	priority = 89
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	# @property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	# def resources(self):
	# 	return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'haplotype_caller': haplotype_caller(case=self.case, cfg=self.cfg)}

	def output(self):
		outputs =  {'filter_germline': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variants', '%s_germline.vcf.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_filter_germline_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		file_base = self.output()['filter_germline'].path.split('.vcf.gz')[0]
		raw_snps = file_base + 'snps.raw.vcf'
		raw_indels = file_base + 'indels.raw.vcf'
		filtered_snps = file_base + 'snps.filtered.vcf'
		filtered_indels = file_base + 'indels.filtered.vcf'
		# select snps
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'SelectVariants', '-R', self.cfg['fasta_file'], '-V', self.input()['haplotype_caller']['haplotype_caller'].path, '--select-type-to-include', 'SNP', '-O', raw_snps]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg)
		else:
			pipeline_utils.command_call(cmd)
		# select indels
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'SelectVariants', '-R', self.cfg['fasta_file'], '-V', self.input()['haplotype_caller']['haplotype_caller'].path, '--select-type-to-include', 'INDEL', '-O', raw_indels]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg)
		else:
			pipeline_utils.command_call(cmd)
		# filter snps
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'VariantFiltration', '-R', self.cfg['fasta_file'], '-V', raw_snps, '-O', filtered_snps]
		for filter_name, filter_expression in [('"filter_1"', '"QD < 2.0"'), ('"filter_2"', '"FS > 60.0"'), ('"filter_3"', '"MQ < 40.0"'), ('"filter_4"', '"MQRankSum < -12.5"'), ('"filter_5"', '"ReadPosRankSum < -8.0"')]:
			cmd += ['--filter-name', filter_name, '--filter-expression', filter_expression]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg)
		else:
			pipeline_utils.command_call(cmd)
		# filter indels
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'VariantFiltration', '-R', self.cfg['fasta_file'], '-V', raw_indels, '-O', filtered_indels]
		for filter_name, filter_expression in [('"filter_1"', '"QD < 2.0"'), ('"filter_2"', '"FS > 200.0"'), ('"filter_3"', '"ReadPosRankSum < -20.0"')]:
			cmd += ['--filter-name', filter_name, '--filter-expression', filter_expression]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg)
		else:
			pipeline_utils.command_call(cmd)
		# combine snps and indels
		cmd = ['java', '-Djava.io.tmpdir=%s' % self.cfg['tmp_dir'], '-jar', '$PICARD', 'MergeVcfs', 'I=%s' % filtered_snps, 'I=%s' % filtered_indels, 'O=%s' % self.output()['filter_germline'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

		for file in [raw_snps, raw_indels, filtered_snps, filtered_indels]:
			os.remove(file)

class mutect2(luigi.Task):
	priority = 87
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
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'Mutect2', '-R', self.cfg['fasta_file'], '--native-pair-hmm-threads', self.cfg['max_threads'], '--germline-resource', self.cfg['gnomad'], '--panel-of-normals', self.input()['mutect2_pon']['mutect2_pon'].path, '-L', self.cfg['library_bed'], '-I', self.input()['T']['preprocess']['bam'].path, '-O', self.output()['mutect2'].path]
		if 'N' in self.cfg['cases'][self.case]:
			cmd += ['-I', self.input()['N']['preprocess']['bam'].path, '-normal', '%s_N' % self.case]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class filter_mutect2(luigi.Task):
	priority = 86
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	# @property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	# def resources(self):
	# 	return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'mutect2': mutect2(case=self.case, cfg=self.cfg)}

	def output(self):
		outputs = {'filter_mutect2': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', '%s_mutect2_filtered.vcf.gz' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_filter_mutect2_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['gatk4', '--java-options', '"-Djava.io.tmpdir=%s"' % self.cfg['tmp_dir'], 'FilterMutectCalls', '-R', self.cfg['fasta_file'], '-V', self.input()['mutect2']['mutect2'].path, '-O', self.output()['filter_mutect2'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class lofreq(luigi.Task):
	priority = 87
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
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class manta(luigi.Task):
	priority = 88
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}, 'N': {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}}

	def output(self):
		outputs = {'manta': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta', 'results', 'variants', 'candidateSmallIndels.vcf.gz')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_manta_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['$MANTA/bin/configManta.py', '--exome', '--referenceFasta', self.cfg['fasta_file'], '--normalBam', self.input()['N']['preprocess']['bam'].path, '--tumorBam', self.input()['T']['preprocess']['bam'].path, '--rundir', os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta')] # TODO add logic for exome to handle multiple sequencing preps
		# pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=1, cfg=self.cfg)
		pipeline_utils.command_call(cmd)
		cmd = [os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'manta', 'runWorkflow.py'), '-j', self.max_threads]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class strelka(luigi.Task):
	priority = 87
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'manta': manta(case=self.case, cfg=self.cfg), 'T': {'preprocess': preprocess.preprocess(case=self.case, sample='T', cfg=self.cfg)}, 'N': {'preprocess': preprocess.preprocess(case=self.case, sample='N', cfg=self.cfg)}}

	def output(self):
		outputs = {'strelka': [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka', 'results', 'variants', 'somatic.%s.vcf.gz' % variant_type)) for variant_type in ['snvs', 'indels']], 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_manta_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		cmd = ['$STRELKA/bin/configureStrelkaSomaticWorkflow.py', '--exome', '--referenceFasta', self.cfg['fasta_file'], '--normalBam', self.input()['N']['preprocess']['bam'].path, '--tumorBam', self.input()['T']['preprocess']['bam'].path, '--indelCandidates', self.input()['manta']['manta'].path, '--rundir', os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka')]
		# pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=1, cfg=self.cfg)
		pipeline_utils.command_call(cmd)
		cmd = [os.path.join(self.cfg['output_dir'], self.case, 'variant_prep', 'strelka', 'runWorkflow.py'), '-m', 'local', '-j', self.cfg['max_threads']]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=12, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class scalpel_discovery(luigi.Task):
	priority = 88
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
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['scalpel-discovery', '--somatic', '--two-pass', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path.split('twopass')[0]), '--format', 'vcf', '--normal', self.input()['N']['preprocess']['bam'].path, '--tumor', self.input()['T']['preprocess']['bam'].path]
		else:
			cmd = ['scalpel-discovery', '--single', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'], '--numprocs', self.cfg['max_threads'], '--dir', os.path.dirname(self.output()['scalpel_discovery'].path), '--format', 'vcf', '--bam', self.input()['T']['preprocess']['bam'].path]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class scalpel_export(luigi.Task):
	priority = 87
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
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['scalpel-export', '--somatic', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'],'-db', self.input()['scalpel_discovery']['scalpel_discovery'].path, '--output-format', 'vcf']
		else:
			cmd = ['scalpel-export', '--single', '--ref', self.cfg['fasta_file'], '--bed', self.cfg['library_bed'],'-db', self.input()['scalpel_discovery']['scalpel_discovery'].path, '--output-format', 'vcf']
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=1, ram=5, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class vcf2maf(luigi.Task):
	priority = 85
	# resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'filter_mutect2': filter_mutect2(case=self.case, cfg=self.cfg)}

	def output(self):
		outputs = {'vcf2maf': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variants', '%s.maf' % self.case)), 'vep': luigi.LocalTarget('%s.vep.vcf' % self.input()['filter_mutect2']['filter_mutect2'].path.split('.vcf')[0]), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_vcf2maf_err.txt' % self.case))}
		for task in outputs:
			if isinstance(outputs[task], luigi.LocalTarget):
				pipeline_utils.confirm_path(outputs[task].path)
		return outputs

	def run(self):
		if self.input()['filter_mutect2']['filter_mutect2'].path.endswith('.gz'):
			input_vcf = self.input()['filter_mutect2']['filter_mutect2'].path.split('.gz')[0]
			with gzip.open(self.input()['filter_mutect2']['filter_mutect2'].path, 'rb') as vcf_in, open(input_vcf, 'wb') as vcf_out:
				shutil.copyfileobj(vcf_in, vcf_out)
		else:
			input_vcf = self.input()['filter_mutect2']['filter_mutect2'].path.endswith('.gz')
		cmd = ['perl', '/root/pipeline/code/source/vcf2maf/vcf2maf.pl', '--ref-fasta', self.cfg['fasta_file'], '--vep-forks', self.cfg['max_threads'], '--input-vcf', input_vcf, '--output-maf', self.output()['vcf2maf'].path, '--tumor-id', '%s_T' % self.case]
		if 'N' in self.cfg['cases'][self.case]:
			cmd += ['--normal-id', '%s_N' % self.case]
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class msisensor(luigi.Task):
	priority = 85
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
		return requirements

	def output(self):
		outputs = {'msisensor': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variants', '%s_updated.msisensor' % self.case)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_msisensor_err.txt' % self.case))}
		if 'N' in self.cfg['cases'][self.case]:
			outputs['msisensor_matched'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'variants', '%s_matched.msisensor' % self.case))
			outputs['err_log_matched'] = luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_msisensor_matched_err.txt' % self.case))
		return outputs

	def run(self):
		cmd = ['msisensor', 'msi', '-d', '/root/pipeline/resources/misc/hg19_microsatellites.list', '-t', self.input()['T']['preprocess']['bam'].path, '-o', self.output()['msisensor'].path, '-l', '1', '-q', '1', '-c', '20'] # , '-e', self.cfg['library_bed'], '-b', self.cfg['max_threads']
		if self.cfg['cluster_exec']:
			pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
		else:
			pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)
		if 'N' in self.cfg['cases'][self.case]:
			cmd = ['msisensor', 'msi', '-d', '/root/pipeline/resources/misc/hg19_microsatellites.list', '-t', self.input()['T']['preprocess']['bam'].path, '-n', self.input()['N']['preprocess']['bam'].path, '-o', self.output()['msisensor_matched'].path, '-l', '1', '-q', '1', '-c', '20'] # , '-e', self.cfg['library_bed'], '-b', self.cfg['max_threads'],
			if self.cfg['cluster_exec']:
				pipeline_utils.cluster_command_call(self, cmd, threads=self.cfg['max_threads'], ram=16, cfg=self.cfg, err_log=self.output()['err_log'].path)
			else:
				pipeline_utils.command_call(cmd, err_log=self.output()['err_log_matched'].path)



class variant_calling(luigi.Task):
	priority = 80
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()

	def requires(self):
		# requirements = {'scalpel_export': scalpel_export(case=self.case, cfg=self.cfg),
		# 'lofreq': lofreq(case=self.case, cfg=self.cfg),
		requirements = {'vcf2maf': vcf2maf(case=self.case, cfg=self.cfg)}
		if 'N' in self.cfg['cases'][self.case]:
			requirements['filter_germline'] = filter_germline(case=self.case, cfg=self.cfg)
		# if 'N' in self.cfg['cases'][self.case]:
		# 	requirements['strelka'] = strelka(case=self.case, cfg=self.cfg)
		return requirements

	def output(self):
		return self.input()

# TODO filter out all germline variants I can find.
