import pathlib
import json
import os
import traceback
import psycopg2
from custom_utils.sql import getConnection
from custom_utils.logger import Logger
from custom_utils.sls import SLS, DYNAMODB

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
LOCAL_VARIANT_CACHE_FILENAME = '.data/all_variants.json'

logger = Logger
sls = SLS
db = DYNAMODB

def get_total_variants_count() -> int:
    connection = getConnection()

    # check on total counts first, also testing connection
    sql_query = 'SELECT COUNT(*) FROM migrate_recent_items WHERE item_type=%s'
    data = ('variant',)
    try:
        logger.debug('Getting cursor...')
        cursor = connection.cursor()
        logger.info(f'executing query to count...')
        cursor.execute(sql_query, data)
        results = cursor.fetchone()
    except psycopg2.Error as error:
        traceback.print_exc()
        print('Error while counting data from PostgreSQL: %s' % error)
        # postgres will abort following transaction anyways, so just close it
        connection.close()
        raise
    logger.info(f'Counted all variants in postgres: {results}')

    return results[0]

def sql_fetch_all_variants_from_pg(start=0, end=15) -> list:
    connection = getConnection()

    # prepare sql 
    sql_query = '''
        SELECT item_type, rownum, item FROM (
            SELECT row_number() over(order by rid, sid) as rownum,
            item_type, 
            item
            FROM migrate_recent_items 
            WHERE item_type=%s
        )a

        WHERE rownum >= %s AND rownum < %s
    '''
    data = ('variant', start, end)
    
    # fetch sql, receive variant objects
    try:
        logger.debug('Getting cursor')
        cursor = connection.cursor()
        logger.info(f'executing query for fetch...')
        cursor.execute(sql_query, data)
        items = cursor.fetchall()
        objects = list(map(lambda item: item[2]['body'], items))
        return objects
    except psycopg2.Error as error:
        traceback.print_exc()
        print('Error while fetching data from PostgreSQL: %s' % error)
        # postgres will abort following transaction anyways, so just close it
        connection.close()
        raise

    return []

def post_all_variants_to_sls(variants: list) -> None:
    # come up with a list of variants that is not in DB, so we know only to POST them (GET is much faster than POST)
    cached_not_in_db_parents_file = pathlib.Path('.data/not_in_db_variants.json')
    if cached_not_in_db_parents_file.exists():
        with cached_not_in_db_parents_file.open('r') as f:
            not_in_db_parents = json.load(f)
    else:
        # get first to see if not in db first
        logger.info('Getting all variants, so we can come up with a list to POST')
        get_results = sls.concurrent_get(variants, include_parent_in_result=True, log=True, reduce_log_mod=100)

        not_in_db_parents = [parent for get_result, parent in get_results if not get_result]
        # not_in_db_parents = variants

        with cached_not_in_db_parents_file.open('w+') as f:
            json.dump(not_in_db_parents, f, indent=2)

    # only POST to those not in db
    logger.info(f'Planning to POST {len(not_in_db_parents)} out of {len(variants)} records (pre-checked by GET)')
    logger.info(f'Will batch POST {len(not_in_db_parents)} items to db...')
    post_results = sls.concurrent_post(not_in_db_parents, raise_http_error=False, log=True)

def fetch_then_post_all_variants():
    # total_variants_count = get_total_variants_count()
    total_variants_count = 16016
    fetch_variant_count_goal = 17070
    fetch_variant_count_goal = total_variants_count if fetch_variant_count_goal > total_variants_count else fetch_variant_count_goal

    # remember to cache or get from cache
    migrate_variants = []
    local_variant_cache_file = pathlib.Path(f'{THIS_FILE_DIR}/{LOCAL_VARIANT_CACHE_FILENAME}')
    if local_variant_cache_file.exists():
        with local_variant_cache_file.open('r') as f:
            migrate_variants = json.load(f)

    # if cached variant less than total, then 
    cached_variants_count = len(migrate_variants)
    if cached_variants_count < fetch_variant_count_goal:
        logger.info(f'cached variant [0,{cached_variants_count-1}], sql fetching [{cached_variants_count}, {fetch_variant_count_goal}) total count goal {fetch_variant_count_goal}')
        migrate_variants = [*migrate_variants, *sql_fetch_all_variants_from_pg(cached_variants_count, fetch_variant_count_goal)]
        logger.info('Writing variants into cache...')
        with local_variant_cache_file.open('w+') as f:
            json.dump(migrate_variants, f)
    else:
        logger.info('All variants cached! Skip sql fetching')

    post_all_variants_to_sls(migrate_variants)

if __name__ == "__main__":
    # db.reset()
    fetch_then_post_all_variants()