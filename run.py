import os
import sys
import argparse
import subprocess
import time
import luigi
# from code import pipeline_utils, global_vars
import pipeline_utils
import preprocess

def run_pipeline(args):
	# import variant_analysis
	sys.path.append(os.getcwd())
	sys.path.append('./')
	


	# timestamp = str(int(time.time()))
	sample_dict = {}

	for sample in os.listdir(args.sample_dir):
		if os.path.isdir(os.path.join(args.sample_dir, sample)):
			sample_dict[sample] = {'T': {}}
			tumor_list = [filename for filename in os.listdir(os.path.join(args.sample_dir, sample, 'tumor')) if 'fastq' in filename]
			sample_dict[sample]['T']['fastq1'] = os.path.join(args.sample_dir, sample, 'tumor', tumor_list[0])
			sample_dict[sample]['T']['fastq2'] = os.path.join(args.sample_dir, sample, 'tumor', tumor_list[1])
			if os.path.exists(os.path.join(args.sample_dir, sample, 'normal')):
				if len(os.listdir(os.path.join(args.sample_dir, sample, 'normal'))) > 0:
					sample_dict[sample]['N'] = {}
					normal_list = [filename for filename in os.listdir(os.path.join(args.sample_dir, sample, 'normal')) if 'fastq' in filename]
					sample_dict[sample]['N']['fastq1'] = os.path.join(args.sample_dir, sample, 'normal', normal_list[0])
					sample_dict[sample]['N']['fastq2'] = os.path.join(args.sample_dir, sample, 'normal', normal_list[1])
	if args.threads_per_sample:
		sample_threads = args.threads_per_sample
	else:
		sample_threads = max(1, args.max_threads//len(sample_dict.keys()))

	worker_scheduler_factory = luigi.interface._WorkerSchedulerFactory()

	class resources(luigi.Config):
	    threads = luigi.IntParameter(default=args.max_threads)

	print(resources().threads)

	luigi.build([cases(sample_dict=sample_dict, project_dir=args.project_dir, sample_threads=sample_threads, cwd=os.getcwd())], workers=args.workers, local_scheduler=args.local_scheduler, worker_scheduler_factory=worker_scheduler_factory) # , workers=args.workers #, scheduler_port=int(args.port)) # workers=sample_threads , resources={'threads': args.max_threads}

class cases(luigi.Task):
	# generated parameters
	sample_dict = luigi.DictParameter()
	project_dir = luigi.Parameter()
	sample_threads = luigi.IntParameter()
	cwd = luigi.Parameter()

	# cfg parameters
	fasta_file = luigi.Parameter()
	dbsnp_indels = luigi.Parameter()
	dbsnp_all = luigi.Parameter()
	platform = luigi.Parameter()
	library = luigi.Parameter()
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
			'fasta_file': self.fasta_file,
			'cases': self.sample_dict,
			'output_dir': os.path.join(self.project_dir, 'output'),
			'max_threads': self.sample_threads,
			'dbsnp_indels': self.dbsnp_indels,
			'dbsnp_all': self.dbsnp_all,
			'platform': self.platform,
			'library': self.library,
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
			'tmp_dir': os.path.join(self.project_dir, 'tmp')
		}
		pipeline_utils.confirm_path(cfg['tmp_dir'])
		pipeline_utils.confirm_path(cfg['output_dir'])

		executions = []
		for case in self.sample_dict:
			executions.append(preprocess.apply_bqsr(cfg=cfg, case=case, sample='T'))
			if 'N' in self.sample_dict[case]:
				executions.append(preprocess.apply_bqsr(cfg=cfg, case=case, sample='N'))

		return executions

		# return [create_mut_mats(max_threads=self.sample_threads, project_dir=self.project_dir, cfg=cfg, case_dict=self.sample_dict)] + \
		# [germline.filter_germline(project_dir=self.project_dir, max_threads=self.sample_threads, cfg=cfg, case_dict=self.sample_dict)] + \
		# [variant_calling.msisensor(max_threads=self.sample_threads, project_dir=self.project_dir, case=case, tumor=self.sample_dict[case]['T'], matched_n=self.sample_dict[case]['N'], cfg=cfg, vcf_path=os.path.join(self.project_dir, 'output', case, 'variants')) for case in self.sample_dict]
	def output(self):
		return self.input()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='wes pipeline parser')
	parser.add_argument('-O', action='store', dest='project_dir', default=os.getcwd(), help='Directory in which the program will create an "output" directory containing all output files.')
	parser.add_argument('-I', action='store', dest='sample_dir', help='Directory containing the input fastq files. Directory contents must be structured as follows:\n[sample_dir]\n\t[sample_name]\n\t\ttumor\n\t\t\t[tumor].fastq.gz\n\t\tnormal\n\t\t\t[normal].fastq.gz')
	parser.add_argument('--threads', action='store', dest='max_threads', default=4, type=int, help='The maximum number of threads available for use. The program will attempt to distribute available threads amongst samples as efficiently and equally as possible.')
	parser.add_argument('--threads_per_sample', action='store', dest='threads_per_sample', default=0, type=int, help='The number of threads to be allowed per sample. If not specified, the program will divide the threads evenly among samples. Specifying it allows you to prioritize single sample completion over equal prcessing of samples simultaneously.')
	parser.add_argument('--workers', action='store', dest='workers', default=1, type=int, help='The number of workers that should be used by the pipeline scheduler (Luigi) to schedule jobs - it will not necessarily determine the number of threads utilized. Recommended: set equal to the number of sequencing files being processed.')
	parser.add_argument('-m', action='store_false', dest='mutect', default=True, help='This flag suppresses analysis with Mutect2')
	parser.add_argument('-d', action='store_false', dest='vardict', default=True, help='This flag suppresses analysis with VarDict')
	parser.add_argument('-f', action='store_false', dest='freebayes', default=True, help='This flag suppresses analysis with FreeBayes')
	parser.add_argument('-v', action='store_false', dest='varscan', default=True, help='This flag suppresses analysis with VarScan')
	parser.add_argument('-c', action='store_false', dest='cnvkit', default=True, help='This flag suppresses analysis with CNVKit')
	parser.add_argument('-s', action='store_false', dest='scalpel', default=True, help='This flag suppresses analysis with Scalpel')
	parser.add_argument('-l', action='store_false', dest='lumpy', default=True, help='This flag suppresses analysis with LUMPY')
	parser.add_argument('-D', action='store_false', dest='delly', default=True, help='This flag suppresses analysis with DELLY')
	parser.add_argument('-w', action='store_false', dest='wham', default=True, help='This flag suppresses analysis with WHAM')
	parser.add_argument('--local_scheduler', action='store_true', dest='local_scheduler', default=False, help='This flag will use the local luigi scheduler as opposed to the luigi server.')
	parser.add_argument('--port', action='store', dest='port', default='8082', help='If using the central luigi scheduler, use this parameter to specify a custom port for the luigi server to operate on (defaults to 8082)')

	args = parser.parse_args()

	# # make sure we're in the correct working directory so relative references work. If not, change to the correct directory
	# if not os.path.exists(os.path.join(os.getcwd(), 'pipeline_code')):
	# 	os.chdir('/'.join(sys.argv[0].split('/')[:-1]))
	# 	# sys.path.append(os.getcwd())
	# if not os.path.exists(os.path.join(os.getcwd(), 'pipeline_code')):
	# 	raise ValueError('you must run script from "wes_pipe" directory, as relative references are used throughout the analysis.')

	# import luigi
	# sample_csv = sys.argv[1]
	# sample_df = pd.read_csv(sample_csv, header=True, index_col='sample_id')
	# sample_dict = {}

	# for sample in sample_df.index.tolist():
	# 	case = sample_df.iloc[sample]['case']
	# 	if case not in sample_dict:
	# 		sample_dict[case] = {'T':'', 'N':''}
	# 	sample_type = sample_df.iloc[sample]['type']
	# 	sample_dict[case][sample_type] = sample_df.iloc[sample]['file']

	# sample_dict = {'ERR031838_1': {'T': '/Users/wep/Documents/Research/Rare_Tumors/pipeline/test_data/ERR031838_1.fastq.gz', 'N': ''}}
	# for case in sample_dict:
	# 	tumor = sample_dict[case]['T']
	# 	matched_n = sample_dict[case]['N']
	# 	luigi.build([variant_calls(case=case, tumor=tumor, matched_n=matched_n)], workers=1, local_scheduler=False)

	# project_dir = sys.argv[1]
	# sample_dir = sys.argv[2]
	# if not args.local_scheduler:
		# print('about to start luigi server...')
		# subprocess.call('python3 /home/wpierson/luigi/src/luigi/bin/luigid --background --pidfile /home/wpierson/projects/wes_pipe/luigi_pidfile.txt --logdir /home/wpierson/projects/wes_pipe --state-path /home/wpierson/projects/wes_pipe/luigi_statepath.txt --port %s &' % args.port)
		# print('Starting luigi server...\n\n')
		# sys.stdout.flush()
		# time.sleep(2)

	run_pipeline(args)
	# luigi.build([bam_processing.cases(max_threads=args.max_threads, project_dir=args.project_dir, sample_dir=args.sample_dir, threads_per_sample=args.threads_per_sample, timestamp=timestamp)], workers=args.workers, local_scheduler=args.local_scheduler)

	# luigi.build([bowtie(fastq_path=fastq_path, sam_path=sam_path, threads=threads, fasta_path=fasta_path), convert_bam(sam_path=sam_path, bam_path=bam_path)], workers=1, local_scheduler=False)