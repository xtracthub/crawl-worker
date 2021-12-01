
from utils.pg_utils import pg_conn
from utils.sqs_utils import get_next_task

from globus_endpoint import GlobusCrawler
from utils.exceptions import *


def main_crawl_loop():

    task = get_next_task()

    crawl_id = task['crawl_id']
    transfer_token = task['transfer_token']
    auth_token = task['auth_token']
    funcx_token = task['funcx_token']

    conn = pg_conn()
    cur = conn.cursor()

    # Step 1: grab tasks
    get_paths_query = f"SELECT path, path_type, endpoint_id, grouper from crawl_paths where crawl_id='{crawl_id}';"
    cur.execute(get_paths_query)

    rows = cur.fetchall()

    paths = []
    for row in rows:
        path = row[0]
        path_type = row[1]
        ep_id = row[2]
        grouper = row[3]

        paths.append({'path': path, 'path_type': path_type, 'ep_id': ep_id})

    # Step 2: for path in paths...
    for path_item in paths:
        if path_item['path_type'].lower() == 'globus':
            # TODO: bring back Google Drive crawler.
            # TODO: bump the db start/finish out to here so we can keep the crawlers clean (+ avoid duplication)
            # TODO: delete messages from queue after receipt
            cr = GlobusCrawler(crawl_id=crawl_id,
                               eid=path_item['ep_id'],
                               trans_token=transfer_token,
                               auth_token=auth_token,
                               funcx_token=funcx_token,
                               path=path_item['path'],
                               grouper_name=grouper)
            tc = cr.get_transfer()

            try:
                cr.crawl(tc)
            except GlobusCrawlException as e:
                # TODO: right here where I should write to DB
                # TODO: return error with nice, descriptive message
                print(f"Exception: {e}")

    # Step 4: Self-Terminate
    exit()


if __name__ == "__main__":
    main_crawl_loop()
