import sys
import os

threads = sys.argv[1]

if __name__ == '__main__':
	with open(os.getenv('LUIGI_CONFIG_PATH'), 'r') as f:
		config = f.read().split('[')
	new_config = []
	for section in config:
		if not section.startswith('resources]'):
			new_config.append(section)
	new_config = '['.join(new_config)
	while not new_config.endswith('\n\n'):
		new_config += '\n'
	new_config += '[resources]\nthreads=%s' % str(args.max_threads)
	with open(os.getenv('LUIGI_CONFIG_PATH'), 'w') as f:
		f.write(new_config)