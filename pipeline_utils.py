import os
import sys
import time
import subprocess
import random
import pickle
import preprocess

# global_max_threads = 0
# thread_file = ''
# working_files = {}
# cwd = ''

def confirm_path(file):
	wait_time = random.uniform(0,0.1)
	time.sleep(wait_time)
	if not os.path.exists(os.path.dirname(file)):
		try:
			os.makedirs(os.path.dirname(file))
		except OSError as exc: # Guard against race condition
			if exc.errno != errno.EEXIST:
				raise

def command_call(cmd, err_log=False):
	start = time.time()
	cmd = [str(x) for x in cmd]
	sys.stdout.flush()
	print('\n' + ' '.join(cmd))
	if not err_log:
		p = subprocess.Popen(' '.join(cmd), shell=True)
		p.communicate()
	else:
		confirm_path(err_log)
		with subprocess.Popen(' '.join(cmd), shell=True, stderr=subprocess.PIPE) as proc:
			with open(err_log, 'wb') as log:
				log.write(proc.stderr.read())
	end = time.time()
	print('Command completed in %s minutes\n' round((end-start)/60, 2))
	
def piped_command_call(cmds, err_log, output_file=False):
	start = time.time()
	cmds = [[str(x) for x in cmd] for cmd in cmds]
	print('\n' + ' '.join(cmds[0]))
	processes = [subprocess.Popen(' '.join(cmds[0]), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)]
	for i, cmd in enumerate(cmds[1:-1]):
		print(' '.join(cmd))
		p = subprocess.Popen(' '.join(cmd), stdin=processes[-1].stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		processes.append(p)
	if output_file:
		print(' '.join(cmds[-1]))
		p = subprocess.Popen(' '.join(cmds[-1]), stdin=processes[-1].stdout, stdout=output_file, stderr=subprocess.PIPE, shell=True)
		processes.append(p)
	else:
		print(' '.join(cmds[-1]))
		p = subprocess.Popen(' '.join(cmds[-1]), stdin=processes[-1].stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		processes.append(p)

	processes[0].stdout.close()
	
	with open(err_log, 'wb') as f:
		f.write(processes[-1].communicate()[1])

	end = time.time()
	print('Command completed in %s minutes\n' round((end-start)/60, 2))

def assign_rg(fastq1, fastq2, case, sample, cfg):
	import gzip
	headers = []
	for file in [fastq1, fastq2]:
		if file.endswith('.gz'):
			with gzip.open(file, 'rb') as f:
				headers.append(f.readline().decode("utf-8"))
		else:
			with open(file, 'r') as f:
				headers.append(f.readline())
	read_groups = []
	for i, header in enumerate(headers):
		header_fields = [x.split(':') for x in header.split('@')[1].split('\n')[0].split(' ')]
		instrument = header_fields[0][0]
		runID = header_fields[0][1]
		flowcellID = header_fields[0][2]
		lane = header_fields[0][3]
		sample_barcode = header_fields[1][3]

		rg = '@RG\\tID:%s.%s\\tPL:%s\\tPU:%s.%s.%s\\tSM:%s_%s\\tLB:%s' % (flowcellID, lane, cfg['platform'], flowcellID, lane, sample_barcode, case, sample, cfg['library'])
		read_groups.append(rg)
	if read_groups[0] == read_groups[1]:
		return read_groups[0]
	else:
		raise Exception('Fastq header mismatch in read pairs from sample: %s_%s' % (case, sample))


# def error_handling(exception):
# 	global working_files
# 	print('Current working files at time of interruption:')
# 	print(pipeline_utils.working_files)
# 	print(cwd)
# 	os.chdir(cwd)
# 	for file in pipeline_utils.working_files:
# 		os.remove(file)
# 	raise exception