import subprocess
import luigi
import os
import sys
import pipeline_utils

class samtools_index(luigi.Task):
	priority = 100
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	def output(self):
		return {'samtools_index': luigi.LocalTarget(self.cfg['fasta_file'] + '.fai')} #, 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'samtools_index_err.txt'))}
	
	def run(self):
		cmd = ['samtools', 'faidx', self.cfg['fasta_file']]
		pipeline_utils.command_call(cmd) #, err_log=self.output()['err_log'].path)

class picard_index(luigi.Task):
	priority = 100
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	def output(self):
		return {'picard_index': luigi.LocalTarget(self.cfg['fasta_file'] + '.dict')} #, 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'picard_index_err.txt'))}
	
	def run(self):
		cmd = ['java', '-jar', '$PICARD', 'CreateSequenceDictionary', 'R=%s' % self.cfg['fasta_file'], 'O=%s' % self.output().path]
		pipeline_utils.command_call(cmd) #, err_log=self.output()['err_log'].path)
		shutil.copyfile(self.output()['picard_index'].path, self.cfg['fasta_file'].split('.fa')[0] + '.dict')


class bwa_index(luigi.Task):
	priority = 100
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	def output(self):
		return {'bwa_index': [luigi.LocalTarget(self.cfg['fasta_file'] + '.amb'), luigi.LocalTarget(self.cfg['fasta_file'] + '.ann'), luigi.LocalTarget(self.cfg['fasta_file'] + '.bwt'), luigi.LocalTarget(self.cfg['fasta_file'] + '.pac'), luigi.LocalTarget(self.cfg['fasta_file'] + '.sa')]} #, 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'bwa_index_err.txt'))}
	
	def run(self):
		cmd = ['bwa', 'index', '-a', 'bwtsw', self.cfg['fasta_file']]
		pipeline_utils.command_call(cmd) #, err_log=self.output()['err_log'].path)

# https://github.com/FelixKrueger/TrimGalore/blob/master/Docs/Trim_Galore_User_Guide.md
class trim(luigi.Task):
	priority = 100
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	def output(self):
		return {'trimgalore': [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_R%s_val_%s.fq.gz' % (self.case, self.sample, n, n))) for n in [1,2]], 'fastqc': [luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'qc', '%s_%s_R%s_val_%s_fastqc.zip' % (self.case, self.sample, n, n))) for n in [1,2]], 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_trim_err.txt' % (self.case, self.sample)))} #TODO - figure out fastqc output filenames

	def run(self):
		cmd = ['trim_galore', '--fastqc', '--fastqc_args "--outdir %s"' % os.path.dirname(self.output()['fastqc'][0].path), '--paired', '-o', os.path.dirname(self.output()['trimgalore'][0].path), '--basename', '%s_%s' % (self.case, self.sample), '--gzip', self.cfg['cases'][self.case][self.sample]['fastq1'], self.cfg['cases'][self.case][self.sample]['fastq2']]
		pipeline_utils.confirm_path(self.output()['fastqc'][0].path)
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class align(luigi.Task):
	priority = 100
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'trim': trim(case=self.case, sample=self.sample, cfg=self.cfg), 'index': bwa_index(cfg=self.cfg)}

	def output(self):
		return {'bwa_mem': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_raw.bam' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_bwa_mem_err.txt' % (self.case, self.sample)))}

	def run(self):
		read_group = pipeline_utils.assign_rg(self.input()['trim']['trimgalore'][0].path, self.input()['trim']['trimgalore'][1].path, self.case, self.sample, self.cfg)
		cmds = [['bwa', 'mem', '-M', '-t', self.cfg['max_threads'], '-R', "'%s'" % read_group, self.cfg['fasta_file'], self.input()['trim']['trimgalore'][0].path, self.input()['trim']['trimgalore'][1].path], ['samtools', 'view', '-bh'], ['samtools', 'sort', '-o', self.output()['bwa_mem'].path]]
		pipeline_utils.piped_command_call(cmds, err_log=self.output()['err_log'].path)

class mark_duplicates(luigi.Task):
	priority = 90
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'align': align(case=self.case, sample=self.sample, cfg=self.cfg)}

	def output(self):
		return {'mark_duplicates': {'bam': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_marked_duplicates.bam' % (self.case, self.sample))), 'metrics': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_marked_dup_metrics.txt' % (self.case, self.sample)))}, 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_mark_duplicates_err.txt' % (self.case, self.sample)))}

	def run(self):
		cmd = ['java', '-jar', '$PICARD', 'MarkDuplicates', 'I=%s' % self.input()['align']['bwa_mem'].path, 'O=%s' % self.output()['mark_duplicates']['bam'].path, 'M=%s' % self.output()['mark_duplicates']['metrics'].path, 'TAGGING_POLICY=All']
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class index_bam(luigi.Task):
	priority = 100
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'mark_duplicates': mark_duplicates(case=self.case, sample=self.sample, cfg=self.cfg)}

	def output(self):
		return {'index': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s.bai' % self.input()['mark_duplicates']['mark_duplicates']['bam'].path)), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_index_bam_err.txt' % (self.case, self.sample)))}

	def run(self):
		cmd = ['samtools', 'index', self.input()['mark_duplicates']['mark_duplicates']['bam'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class realigner_target(luigi.Task):
	priority = 90
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {}
		for case in self.cfg['cases']:
			requirements[case] = {'T': {'mark_duplicates': mark_duplicates(case=case, sample='T', cfg=self.cfg), 'index_bam': index_bam(case=case, sample='T', cfg=self.cfg)}}
			# executions.append(preprocess.apply_bqsr(cfg=self.cfg, case=case, sample='T'))
			if 'N' in self.cfg['cases'][case]:
				requirements[case]['N'] = {'mark_duplicates': mark_duplicates(case=case, sample='N', cfg=self.cfg), 'index_bam': index_bam(case=case, sample='N', cfg=self.cfg)}

		return requirements

	def output(self):
		# return {'realigner_target': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_realigner_targets.intervals' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_realigner_target_err.txt' % (self.case, self.sample)))}
		return {'realigner_target': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'preprocess', 'all_samples_realigner_targets.intervals')), 'file_map': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'preprocess', 'all_samples_realigner.map')), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'realigner_target_err.txt'))}
	def run(self):
		cmd = ['java', '-jar', '$GATK3', '-T', 'RealignerTargetCreator', '-R', self.cfg['fasta_file'], '--known', self.cfg['dbsnp_indels'], '-nct', self.cfg['max_threads'], '-o', self.output()['realigner_target'].path]
		file_map = []
		for case in self.input():
			for sample in self.input()[case]:
				filename = self.input()[case][sample]['mark_duplicates']['mark_duplicates']['bam'].path
				realigned_filename = filename.split('marked_duplicates.bam')[0] + 'realigned.bam'
				file_map.append('%s\t%s' % (os.path.basename(filename), realigned_filename))
				cmd += ['-I', filename]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)
		pipeline_utils.confirm_path(self.output()['file_map'].path)
		with open(self.output()['file_map'].path, 'w') as f:
			f.write('\n'.join(file_map))

class indel_realigner(luigi.Task):
	priority = 90
	cfg = luigi.DictParameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		requirements = {'realigner_target': realigner_target(cfg=self.cfg), 'cases': {}}
		for case in self.cfg['cases']:
			requirements['cases'][case] = {'T': {'mark_duplicates': mark_duplicates(case=case, sample='T', cfg=self.cfg), 'index_bam': index_bam(case=case, sample='T', cfg=self.cfg)}}
			if 'N' in self.cfg['cases'][case]:
				requirements['cases'][case]['N'] = {'mark_duplicates': mark_duplicates(case=case, sample='N', cfg=self.cfg), 'index_bam': index_bam(case=case, sample='N', cfg=self.cfg)}
		return requirements

	def output(self):
		outputs = {'indel_realigner': {}, 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], 'all_samples', 'log', 'indel_realigner_err.txt'))}
		file_map = []
		for case in self.input()['cases']:
			for sample in self.input()['cases'][case]:
				filename = self.input()['cases'][case][sample]['mark_duplicates']['mark_duplicates']['bam'].path
				realigned_filename = filename.split('marked_duplicates.bam')[0] + 'realigned.bam'
				if case not in outputs['indel_realigner']:
					outputs['indel_realigner'][case] = {}
				outputs['indel_realigner'][case][sample] = luigi.LocalTarget(realigned_filename)
		return outputs

	def run(self):
		cmd = ['java', '-jar', '$GATK3', '-T', 'IndelRealigner', '-R', self.cfg['fasta_file'], '--nWayOut', self.input()['realigner_target']['file_map'].path, '-known', self.cfg['dbsnp_indels'], '--consensusDeterminationModel', 'USE_SW', '-nct', self.cfg['max_threads'], '--targetIntervals', self.input()['realigner_target']['realigner_target'].path]
		for case in self.input()['cases']:
			for sample in self.input()['cases'][case]:
				filename = self.input()['cases'][case][sample]['mark_duplicates']['mark_duplicates']['bam'].path
				cmd += ['-I', filename]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)
		# self.input()['realigner_target']['file_map'].remove()

class base_recalibrator(luigi.Task):
	priority = 90
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	@property # This is necessary to assign a dynamic value to the 'threads' resource within a task
	def resources(self):
		return {'threads': self.cfg['max_threads']}

	def requires(self):
		return {'indel_realigner': indel_realigner(cfg=self.cfg)}

	def output(self):
		return {'base_recalibrator': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_recal_data.table' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_base_recalibrator_err.txt' % (self.case, self.sample)))}

	def run(self):
		cmd = ['java', '-jar', '$GATK3', '-T', 'BaseRecalibrator', '-I', self.input()['indel_realigner']['indel_realigner'][self.case][self.sample].path, '-R', self.cfg['fasta_file'], '-knownSites', self.cfg['dbsnp_all'], '-nct', self.cfg['max_threads'], '-o', self.output()['base_recalibrator'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)

class apply_bqsr(luigi.Task):
	priority = 90
	resources = {'threads': 1}
	cfg = luigi.DictParameter()

	case = luigi.Parameter()
	sample = luigi.Parameter()

	def requires(self):
		return {'base_recalibrator': base_recalibrator(case=self.case, sample=self.sample, cfg=self.cfg), 'indel_realigner': indel_realigner(cfg=self.cfg)}

	def output(self):
		return {'apply_bqsr': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'preprocess', '%s_%s_recalibrated.bam' % (self.case, self.sample))), 'err_log': luigi.LocalTarget(os.path.join(self.cfg['output_dir'], self.case, 'log', '%s_%s_apply_bqsr_err.txt' % (self.case, self.sample)))}

	def run(self):
		cmd = ['java', '-jar', '$GATK3', '-T', 'PrintReads', '-I', self.input()['indel_realigner']['indel_realigner'][self.case][self.sample].path, '-R', self.cfg['fasta_file'], '-BQSR', self.input()['base_recalibrator']['base_recalibrator'].path, '-o', self.output()['apply_bqsr'].path]
		pipeline_utils.command_call(cmd, err_log=self.output()['err_log'].path)