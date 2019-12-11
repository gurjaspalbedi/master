# from dependency_manager import Dependencies
# from configuration import worker_list, inverted_index_path, word_count_path,\
# word_count_map, word_count_reducer, inverted_index_map, inverted_index_reducer, mapper_tasks_path, \
# reducer_task_path, configuration_path

# log = Dependencies.log()

# def welcome():
# 	log.write(f'Following Operations can be performed. Type given number or command', 'debug')
# 	log.write(f'1. Init Default Cluster having nodes {repr(worker_list[0])}', 'debug')
# 	log.write(f'2. Run Map Reduce WORD COUNT: this is same as executing run_mapred(0,0)', 'debug')
# 	log.write(f'3. Run Map Reduce INVERTED INDEX: this is same as executing run_mapred(0,1)', 'debug')
# 	log.write(f'4. Destroy Default Cluster', 'debug')
# 	log.write(f'OR type any of the command <init_cluster(cluster_id)>, <run_mapred(cluster_id, task_id)>, <destory_cluter(cluster_id)>', 'debug')
# 	log.write('============================CONFIGURATION INFO============================')
# 	log.write('Master configuration file path: worker_server/configuration.py')
# 	log.write('Data Store configuration file path: data_store/configuration.py ')
# 	log.write(f'Location of MAP function:')
# 	log.write(f'Inverted Index: {inverted_index_map}')
# 	log.write(f'Word Cound: {word_count_map}')
# 	log.write(f'\n\n')
# 	log.write(f'Location of REDUCER function:')
# 	log.write(f'Inverted Index: {inverted_index_reducer}')
# 	log.write(f'Word Cound: {word_count_reducer}')