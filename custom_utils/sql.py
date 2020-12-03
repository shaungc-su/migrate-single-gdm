import psycopg2
import yaml
import os
from custom_utils.logger import Logger
from custom_utils.config import config_data

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))

logger = Logger

def getConnection(type='ec2'):
    logger.info('Getting connection object')
    if (type == 'local'):
        return psycopg2.connect(user=config_data['db']['local']['user'],
                                password=config_data['db']['local']['password'],
                                host=config_data['db']['local']['host'],
                                port=config_data['db']['local']['port'],
                                database=config_data['db']['local']['database'])
    elif (type == 'ec2'):
        logger.debug(f"Connecting to ec2 postgres, psw={config_data['db']['ec2']['password']}")
        return psycopg2.connect(user=config_data['db']['ec2']['user'],
                                password=config_data['db']['ec2']['password'],
                                host=config_data['db']['ec2']['host'],
                                port=config_data['db']['ec2']['port'],
                                database=config_data['db']['ec2']['database'])
    else:
        raise Exception("Bad instance type")