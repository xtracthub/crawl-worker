
import json
import time
import requests
import threading

from utils.pg_utils import pg_conn
from utils.sqs_utils import get_next_task, get_dev_status

from globus_endpoint import GlobusCrawler
from utils.exceptions import *


class HeartbeatThread:
    def __init__(self):
        self.to_terminate = False
        self.is_dev = get_dev_status()
        self.crawl_id_lock = False
        self.current_crawl_id = None

        # TODO: these are the same. See if we need to separate.
        if self.is_dev:
            self.hb_url = "http://xtractcrawler5-env.eba-akbhvznm.us-east-1.elasticbeanstalk.com/heartbeat"
        else:
            self.hb_url = "http://xtractcrawler5-env.eba-akbhvznm.us-east-1.elasticbeanstalk.com/heartbeat"

    def hb_thread(self):
        print("Starting heartbeat thread...")
        while True:
            time.sleep(5)

            if self.current_crawl_id is None:
                print(f"[hb] No current crawl_id!")
                continue

            print(f"[hb] Sending hb...")
            resp = requests.get(self.hb_url, json={'crawl_id': self.current_crawl_id})
            hb_return_obj = resp.json()

            # print(hb_return_obj.json())
            print(hb_return_obj)

            ret_crawl_id = hb_return_obj['crawl_id']
            hb_return_status = hb_return_obj['status']

            if ret_crawl_id != self.current_crawl_id:
                print(f"[hb] We are no longer tracking crawl_id {ret_crawl_id}. Continuing...")
                continue

            if hb_return_status == "STOP":
                self.to_terminate = True

                # Checking again because of potential race condition.
                while True:
                    if self.crawl_id_lock:
                        print(f"Crawl ID is locked. Sleeping 3 seconds and trying again...")
                        time.sleep(3)
                        continue
                    else:
                        self.crawl_id_lock = True  # CRAWL_ID IS LOCKED.
                        break

                if ret_crawl_id != self.current_crawl_id:
                    print(f"[hb] We are no longer tracking crawl_id {ret_crawl_id}. Continuing...")
                    continue
                else:
                    self.current_crawl_id = None

                self.crawl_id_lock = False

    def start_hb_thread(self):
        th = threading.Thread(target=self.hb_thread, args=())
        th.daemon = True
        th.start()

    def set_crawl_id(self, crawl_id):
        # Checking again because of potential race condition.
        while True:
            if self.crawl_id_lock:
                print(f"Crawl ID is locked. Sleeping 3 seconds and trying again...")
                time.sleep(3)
                continue
            else:
                self.crawl_id_lock = True  # CRAWL_ID IS LOCKED.
                break
        self.current_crawl_id = crawl_id
        self.to_terminate = False
        self.crawl_id_lock = False


def main_crawl_loop():

    hb = HeartbeatThread()
    hb.start_hb_thread()

    while True:

        try:
            task = get_next_task()
        except KeyError:
            print(f"UNABLE TO FIND A TASK. SLEEPING FOR 5s AND CONTINUING...")
            time.sleep(5)
            continue

        crawl_id = task['crawl_id']
        print(f"Retrieved crawl job for ID: {crawl_id}")
        transfer_token = task['transfer_token']
        auth_token = task['auth_token']
        funcx_token = task['funcx_token']

        # Set crawl_id in heartbeater.
        # This will also set hb.to_terminate to False.
        hb.set_crawl_id(crawl_id=crawl_id)

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

        hb.current_crawl_id = None

        if hb.to_terminate:
            print(f"Need to inject this logic INTO the crawlers. This needs to pause the crawl and just continue...")
            continue
        # Step 4: Self-Terminate
        # exit()  # TODO: tyler terminated this on 12/1/21. Should see if this is ever needed again.


if __name__ == "__main__":
    main_crawl_loop()
