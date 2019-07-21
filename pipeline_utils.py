import os
import sys
import time
import subprocess
import random
import pickle
import preprocess
import errno

# global_max_threads = 0
# thread_file = ''
# working_files = {}
# cwd = ''

def confirm_path(file):
	# wait_time = random.uniform(0,0.1)
	# time.sleep(wait_time)
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
	print('Command completed in %s minutes\n' % round((end-start)/60, 2))
	if err_log:
		with open(err_log, 'a') as f:
			f.write('\n\n***\nCommand completed in %s minutes\n***' % round((end-start)/60, 2))
	
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
	print('Command completed in %s minutes\n' % round((end-start)/60, 2))
	with open(err_log, 'a') as f:
		f.write('\n\n***\nCommand completed in %s minutes\n***' % round((end-start)/60, 2))

def cluster_command_call(task, cmd, threads, ram, cfg, err_log=False, refresh_time=60):
	print(task.task_id)
	jobid, task_script_file, job_script_file = submit_job(cmd, threads, ram, cfg, task.task_id)
	queue_start = time.time()
	run_start = 0
	while True:
		update_time = time.time()
		status = get_job_status(jobid)
		if status == 'queue':
			task.set_status_message('In job queue for %s mins' % round(update_time - queue_start, 2))
			time.sleep(refresh_time)
		elif status == 'run':
			if run_start == 0:
				run_start = update_time
			task.set_status_message('Running for %s mins' % round(update_time - run_start, 2))
			time.sleep(refresh_time)
		else:
			done = time.time()
			break
	queue_time = round(run_start - queue_start, 2)
	run_time = round(done - run_start, 2)
	total_time = round(done - queue_start, 2)
	if err_log:
		with open(job_script_file + jobid, 'r') as f:
			with open(err_log, 'w') as f2:
				f2.write(f.read())

	return (total_time, run_time, queue_time)

def submit_job(cmd, threads, ram, cfg, task_id):
	cwd = os.getcwd()
	cmd = ' '.join(cmd)
	for (sys_path, docker_path) in [(cfg['resources_dir'], '/root/pipeline/resources'), (cfg['code_dir'], '/root/pipeline/code'), (cfg['input_dir'], '/root/input'), (cfg['output_dir'], '/root/output')]:
		cmd = cmd.replace(os.path.dirname(sys_path), os.path.dirname(docker_path))

	task_script = 'echo "export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.8.0-openjdk-amd64/lib/amd64/jli" >> /root/.profile\n'
	task_script += 'echo "search som.ucsf.edu ucsf.edu ucsfmedicalcenter.org medschool.ucsf.edu campus.net" > /etc/resolv.conf\n'
	task_script += 'echo "nameserver 128.218.87.135" >> /etc/resolv.conf\n'
	task_script += 'echo "nameserver 64.54.144.10" >> /etc/resolv.conf\n'
	task_script += 'echo "nameserver 128.218.224.175" >> /etc/resolv.conf\n'
	task_script += '. /root/.profile\n'
	task_script += 'alias python=/usr/bin/python3.6\n'
	task_script += 'alias python3=/usr/bin/python3.6\n'
	task_script += ' '.join(cmd)
	sys_task_script_file = os.path.join(cfg['output_dir'], 'task_scripts', '%s.sh' % task_id)
	confirm_path(sys_task_script_file)
	docker_task_script_file = os.path.join('/root', 'output', 'task_scripts', '%s.sh' % task_id)

	with open(sys_task_script_file, 'w') as f:
		f.write(task_script)

	job_script = 'module load CBC python udocker\n'
	job_script += 'chmod -755 %s' % sys_task_script_file
	job_script += 'udocker run --novol=/etc/host.conf --novol=/etc/resolv.conf -v %s:/root/pipeline/resources -v %s:/root/pipeline/code -v %s:/root/input -v $%s/output:/root/output seq_pipeline %s' % (cfg['resources_dir'], cfg['code_dir'], cfg['input_dir'], cfg['output_dir'], docker_task_script_file)
	job_script_file = os.path.join(cfg['output_dir'], 'job_scripts', '%s.sh' % task_id)
	confirm_path(job_script_file)
	with open(job_script_file, 'w') as f:
		f.write(job_script)

	os.chdir(os.path.dirname(job_script_file))
	p = subprocess.Popen('qsub -l vmem=%sgb -l nodes=1:ppn=%s %s' % (str(ram), str(threads), job_script_file), stdout=subprocess.PIPE, shell=True)
	jobid = p.stdout.read().decode('utf-8').split('.')[0]
	os.chdir(cwd)
	return jobid, sys_task_script_file, job_script_file

def get_job_status(jobid):
	p = subprocess.Popen('qstat -alt', stdout=subprocess.PIPE, shell=True)
	qstat_out = p.stdout.read().decode('utf-8')
	if jobid in qstat_out:
		status = qstat_out[qstat_out.find('1383429'):].split('\n')[0].split()[-2]
		if status == 'R':
			return 'run'
		else:
			return 'queue'
	else:
		return 'done'

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