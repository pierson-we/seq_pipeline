import os
import sys
import argparse
import subprocess
import time
# from code import pipeline_utils, global_vars
import luigi
import pipeline_utils
import preprocess
import snv_indel
import cnv_sv

def run_pipeline(args):
	# import variant_analysis
	sys.path.append(os.getcwd())
	sys.path.append('./')
	
	# timestamp = str(int(time.time()))
	sample_dict = {}
	sample_type_dict = {'normal': 'N', 'tumor': 'T'}
	for sample in os.listdir(args.sample_dir):
		if os.path.isdir(os.path.join(args.sample_dir, sample)):
			sample_dict[sample] = {}
			for sample_type in ['normal', 'tumor']:
				if os.path.exists(os.path.join(args.sample_dir, sample, sample_type)):
					lanes = os.listdir(os.path.join(args.sample_dir, sample, sample_type))
					if len(lanes) > 0:
						sample_dict[sample][sample_type_dict[sample_type]] = {}
						for lane in os.listdir(os.path.join(args.sample_dir, sample, sample_type)):
							if os.path.isdir(os.path.join(args.sample_dir, sample, sample_type, lane)):
								sample_dict[sample][sample_type_dict[sample_type]][lane] = {}
								fastq_list = [filename for filename in os.listdir(os.path.join(args.sample_dir, sample, sample_type, lane)) if 'fastq' in filename]
								if len(fastq_list) > 2:
									raise Exception('More than 2 fastq files present for Sample %s_%s Lane %s' %(sample, sample_type_dict[sample_type]), lane)
								sample_dict[sample][sample_type_dict[sample_type]][lane]['fastq1'] = os.path.join(args.sample_dir, sample, sample_type, lane, fastq_list[0])
								sample_dict[sample][sample_type_dict[sample_type]][lane]['fastq2'] = os.path.join(args.sample_dir, sample, sample_type, lane, fastq_list[1])
	if args.threads_per_sample:
		sample_threads = args.threads_per_sample
	else:
		sample_threads = max(1, args.max_threads//len(sample_dict.keys()))

	worker_scheduler_factory = luigi.interface._WorkerSchedulerFactory()
	# luigi.interface.core.threads = luigi.parameter.IntParameter(default=args.max_threads, description='total number of threads available for use by the pipeline', config_path=dict(section='resources', name='threads'),)

	luigi.build([cases(sample_dict=sample_dict, project_dir=args.project_dir, global_max_threads=args.max_threads, max_threads=sample_threads, input_dir=args.sample_dir, cwd=os.getcwd(), cluster_exec=args.cluster_exec)], workers=args.workers, local_scheduler=args.local_scheduler, worker_scheduler_factory=worker_scheduler_factory) # , workers=args.workers #, scheduler_port=int(args.port)) # workers=sample_threads , resources={'threads': args.max_threads}

class cases(luigi.Task):
	# generated parameters
	sample_dict = luigi.DictParameter()
	project_dir = luigi.Parameter()
	global_max_threads = luigi.IntParameter()
	max_threads = luigi.IntParameter()
	input_dir = luigi.Parameter()
	cwd = luigi.Parameter()
	cluster_exec = luigi.BoolParameter()

	# cfg parameters
	resources_dir = luigi.Parameter()
	code_dir = luigi.Parameter()
	fasta_file = luigi.Parameter()
	germline_indels = luigi.Parameter()
	germline_all = luigi.Parameter()
	germline_genes = luigi.Parameter()
	gnomad = luigi.Parameter()
	platform = luigi.Parameter()
	library = luigi.Parameter()
	library_bed = luigi.Parameter()
	# library_bed = luigi.Parameter()
	# gatk4_location = luigi.Parameter()
	# gatk3_location = luigi.Parameter()
	# known_vcf = luigi.Parameter()
	# germline_resource = luigi.Parameter()
	# picard_location = luigi.Parameter()
	# vardict_location = luigi.Parameter()
	# mills = luigi.Parameter()
	# kg = luigi.Parameter()
	# #omni = luigi.Parameter()
	# #hapmap = luigi.Parameter()
	# library_prep = luigi.Parameter()
	# platform = luigi.Parameter()
	# base_name = luigi.Parameter()
	# samtools_location = luigi.Parameter()
	# bowtie_build_location = luigi.Parameter()
	# bowtie_location = luigi.Parameter()
	# fastqc_location = luigi.Parameter()
	# trim_location = luigi.Parameter()
	# insert_size = luigi.Parameter()
	# freebayes_location = luigi.Parameter()
	# vcffilter_location = luigi.Parameter()
	# cnvkit_location = luigi.Parameter()
	# refFlat = luigi.Parameter()
	# cnvkit_seg_method = luigi.Parameter()
	# cnvkit_genemetrics_threshold = luigi.Parameter()
	# cnvkit_genemetrics_minprobes = luigi.Parameter()
	# pindel_min_reads = luigi.IntParameter()
	# pindel_min_qual = luigi.IntParameter()
	# pindel_max_inv_length = luigi.IntParameter()
	# genmap = luigi.Parameter()
	# exons_bed = luigi.Parameter()

	def requires(self):
		cfg = {
			'resources_dir': self.resources_dir, 
			'code_dir': self.code_dir,
			'input_dir': self.input_dir,
			'fasta_file': self.fasta_file,
			'cases': self.sample_dict,
			'output_dir': os.path.join(self.project_dir, 'output'),
			'global_max_threads': self.global_max_threads,
			'max_threads': self.max_threads,
			'germline_indels': self.germline_indels,
			'germline_all': self.germline_all,
			'germline_genes': self.germline_genes,
			'gnomad': self.gnomad,
			'platform': self.platform,
			'library': self.library,
			'library_bed': self.library_bed,
			'cluster_exec': self.cluster_exec,
			# 'library_bed': self.library_bed,
			# 'gatk4_location': self.gatk4_location,
			# 'gatk3_location': self.gatk3_location,
			# 'known_vcf': self.known_vcf,
			# 'germline_resource': self.germline_resource,
			# 'picard_location': self.picard_location,
			# 'vardict_location': self.vardict_location,
			# 'mills': self.mills,
			# 'kg': self.kg,
			# #'omni': self.omni,
			# #'hapmap': self.hapmap,
			# 'library_prep': self.library_prep,
			# 'platform': self.platform,
			# 'base_name': self.base_name,
			# 'samtools_location': self.samtools_location,
			# 'bowtie_build_location': self.bowtie_build_location,
			# 'bowtie_location': self.bowtie_location,
			# 'fastqc_location': self.fastqc_location,
			# 'trim_location': self.trim_location,
			# 'insert_size': self.insert_size,
			# 'freebayes_location': self.freebayes_location,
			# 'vcffilter_location': self.vcffilter_location,
			# 'cnvkit_location': self.cnvkit_location,
			# 'refFlat': self.refFlat,
			# 'cnvkit_seg_method': self.cnvkit_seg_method,
			# 'cnvkit_genemetrics_threshold': self.cnvkit_genemetrics_threshold,
			# 'cnvkit_genemetrics_minprobes': self.cnvkit_genemetrics_minprobes,
			# 'pindel_min_reads': self.pindel_min_reads,
			# 'pindel_min_qual': self.pindel_min_qual,
			# 'pindel_max_inv_length': self.pindel_max_inv_length,
			# 'genmap': self.genmap,
			# 'exons_bed': self.exons_bed,
			'tmp_dir': os.path.join(self.project_dir, 'output', 'tmp')
		}
		pipeline_utils.confirm_path(cfg['tmp_dir'])
		pipeline_utils.confirm_path(cfg['output_dir'])

		executions = []
		for case in self.sample_dict:
			executions.append(snv_indel.variant_calling(cfg=cfg, case=case))
			executions.append(snv_indel.msisensor(cfg=cfg, case=case))
			executions.append(cnv_sv.facets_snp_pileup(cfg=cfg, case=case))

		return executions

		# return [create_mut_mats(max_threads=self.sample_threads, project_dir=self.project_dir, cfg=cfg, case_dict=self.sample_dict)] + \
		# [germline.filter_germline(project_dir=self.project_dir, max_threads=self.sample_threads, cfg=cfg, case_dict=self.sample_dict)] + \
		# [variant_calling.msisensor(max_threads=self.sample_threads, project_dir=self.project_dir, case=case, tumor=self.sample_dict[case]['T'], matched_n=self.sample_dict[case]['N'], cfg=cfg, vcf_path=os.path.join(self.project_dir, 'output', case, 'variants')) for case in self.sample_dict]
	def output(self):
		return self.input()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='wes pipeline parser')
	parser.add_argument('-C', '--cluster', action='store_true', dest='cluster_exec', default=False, help='Cluster execution mode (qsub task execution)')
	parser.add_argument('-O', action='store', dest='project_dir', default=os.getcwd(), help='Directory in which the program will create an "output" directory containing all output files.')
	parser.add_argument('-I', action='store', dest='sample_dir', required=True, help='Directory containing the input fastq files. Directory contents must be structured as follows:\n[sample_dir]\n\t[sample_name]\n\t\ttumor\n\t\t\t[tumor].fastq.gz\n\t\tnormal\n\t\t\t[normal].fastq.gz')
	parser.add_argument('-t', '--threads', action='store', dest='max_threads', default=4, type=int, help='The maximum number of threads available for use. The program will attempt to distribute available threads amongst samples as efficiently and equally as possible.')
	parser.add_argument('-s', '--threads_per_sample', action='store', dest='threads_per_sample', default=1, type=int, help='The number of threads to be allowed per sample. If not specified, the program will divide the threads evenly among samples. Specifying it allows you to prioritize single sample completion over equal prcessing of samples simultaneously.')
	parser.add_argument('-w', '--workers', action='store', dest='workers', default=1, type=int, help='The number of workers that should be used by the pipeline scheduler (Luigi) to schedule jobs - it will not necessarily determine the number of threads utilized. Recommended: set equal to the number of sequencing files being processed.')
	# parser.add_argument('-m', action='store_false', dest='mutect', default=True, help='This flag suppresses analysis with Mutect2')
	# parser.add_argument('-d', action='store_false', dest='vardict', default=True, help='This flag suppresses analysis with VarDict')
	# parser.add_argument('-f', action='store_false', dest='freebayes', default=True, help='This flag suppresses analysis with FreeBayes')
	# parser.add_argument('-v', action='store_false', dest='varscan', default=True, help='This flag suppresses analysis with VarScan')
	# parser.add_argument('-c', action='store_false', dest='cnvkit', default=True, help='This flag suppresses analysis with CNVKit')
	# parser.add_argument('-s', action='store_false', dest='scalpel', default=True, help='This flag suppresses analysis with Scalpel')
	# parser.add_argument('-l', action='store_false', dest='lumpy', default=True, help='This flag suppresses analysis with LUMPY')
	# parser.add_argument('-D', action='store_false', dest='delly', default=True, help='This flag suppresses analysis with DELLY')
	# parser.add_argument('-w', action='store_false', dest='wham', default=True, help='This flag suppresses analysis with WHAM')
	parser.add_argument('-l', '--local_scheduler', action='store_true', dest='local_scheduler', default=False, help='This flag will use the local luigi scheduler as opposed to the luigi server.')
	parser.add_argument('-p', '--port', action='store', dest='port', default='8082', help='If using the central luigi scheduler, use this parameter to specify a custom port for the luigi server to operate on (defaults to 8082)')

	args = parser.parse_args()

	run_pipeline(args)

	# with open(os.getenv('LUIGI_CONFIG_PATH'), 'w') as f:
	# 	f.write('['.join(config))
	# luigi.build([bam_processing.cases(max_threads=args.max_threads, project_dir=args.project_dir, sample_dir=args.sample_dir, threads_per_sample=args.threads_per_sample, timestamp=timestamp)], workers=args.workers, local_scheduler=args.local_scheduler)

	# luigi.build([bowtie(fastq_path=fastq_path, sam_path=sam_path, threads=threads, fasta_path=fasta_path), convert_bam(sam_path=sam_path, bam_path=bam_path)], workers=1, local_scheduler=False)