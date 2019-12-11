# -*- coding: utf-8 -*-

# Following blog was used for the implementation of the logggin in the application
#https://realpython.com/python-logging/
import logging
from termcolor import colored

class WorkerLogger:
    
    def __init__(self):
        logging.basicConfig(level=logging.INFO,filename='master_log.log', filemode='a', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
    def write(self, message, level="warning"):
        
        if level == "debug":
            print(colored(message, 'green'))
            logging.debug(message)
        elif level == "info":
            print(colored(message, 'white'))
            logging.info(message)
        elif level == "warning":
            print(colored(message, 'yellow'))
            logging.warning(message)
        elif level == "error":
            print(colored(message, 'red'))
            logging.error(message)
        elif level == "critical":
            print(colored(message, 'red'))
            logging.critical(message)
        
        
        
        
        
    
    
    