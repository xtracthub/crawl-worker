
import datetime
import threading
import os
import boto3
from .base import Crawler
from crawlers.utils.gdrive_utils import get_next_page, generate_drive_connection
from crawlers.groupers import simple_ext_grouper
from queue import Queue
import time
import json


class GoogleDriveCrawler(Crawler):

    def __init__(self, crawl_id, creds, grouper_name="file"):
        Crawler.__init__(self)
        self.crawl_id = crawl_id
        self.creds = creds
        self.grouper_name = grouper_name

        # self.count_groups_crawled = 0
        self.count_files_crawled = 0
        self.count_bytes_crawled = None
        self.count_groups_crawled = self.count_files_crawled

        self.crawl_tallies = None

        self.active_commits = 0

        self.commit_threads = 5
        self.families_to_enqueue = Queue()

        self.numdocs = 0
        self.numfiles = 0

        self.crawl_start = time.time()
        self.crawl_end = None

        print("Generating connection to Google Drive API...")
        self.drive_conn = generate_drive_connection(self.creds)
        print("Connection to Drive API successful!")

        self.crawl_status = "STARTING"

        self.total_grouping_time = 0
        self.total_graphing_time = 0
        self.globus_network_time = 0
        self.total_auth_time = 0

        self.pre_commit_time = 0

        # TODO: this really needs to be part of its own data class bc copied and pasted from globus_base
        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
        print(f"Creating queue for crawl_id: {self.crawl_id}")
        queue = self.client.create_queue(QueueName=f"crawl_{str(self.crawl_id)}")

        if queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.queue_url = queue["QueueUrl"]
        else:
            raise ConnectionError("Received non-200 status from SQS!")

        self.sqs_push_threads = {}
        self.thr_ls = []
        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        print(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

    def enqueue_loop(self, thr_id):

        print("In enqueue loop!")
        while True:
            insertables = []

            # If empty, then we want to return.
            if self.families_to_enqueue.empty():
                # If ingest queue empty, we can demote to "idle"
                if self.crawl_status == "COMMITTING":
                    self.sqs_push_threads[thr_id] = "IDLE"
                    print(f"Thread {thr_id} is committing and idle!")
                    time.sleep(0.25)

                    # NOW if all threads idle, then return!
                    if all(value == "IDLE" for value in self.sqs_push_threads.values()):
                        self.commit_completed = True
                        self.crawl_status = "COMPLETED"
                        self.crawl_end = time.time()
                        self.commit_time = self.crawl_end - self.pre_commit_time
                        print(f"Thread {thr_id} is terminating!")
                        print(f"Grouping time: {self.total_grouping_time}")
                        print(f"Google Req time: {self.globus_network_time}")
                        print(f"Total commit time: {self.commit_time}")
                        print(f"Total time: {self.crawl_start - self.crawl_end}")
                        return 0
                time.sleep(1)
                continue

            self.sqs_push_threads[thr_id] = "ACTIVE"

            # Remove up to n elements from queue, where n is current_batch.
            current_batch = 1
            while not self.families_to_enqueue.empty() and current_batch < 10:
                insertables.append(self.families_to_enqueue.get())
                self.active_commits -= 1
                current_batch += 1

            # print(f"Insertables: {insertables}")


            try:
                response = self.client.send_message_batch(QueueUrl=self.queue_url,
                                                          Entries=insertables)
                print(f"SQS response on insert: {response}")
            except Exception as e:  # TODO: too vague
                print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

    def crawl(self, tc=None):  # TODO: Find clean way to remove transfer_client requirement from .crawl() function.
        # TODO: Properly generate the queues for these.
        all_files = []
        next_page_token = None
        starting = True

        grouper = simple_ext_grouper.SimpleExtensionGrouper(creds=self.creds)

        self.crawl_status = "PROCESSING"
        while True:

            if not next_page_token and starting:
                # Call the Drive v3 API
                tc = time.time()
                results = get_next_page(self.drive_conn, None)
                starting = False
                td = time.time()

                self.globus_network_time += td-tc

            else:
                te = time.time()
                results = get_next_page(self.drive_conn, next_page_token)
                tf = time.time()
                self.globus_network_time += tf - te
            items = results.get('files', [])
            next_page_token = results.get("nextPageToken", [])

            clean_mdata = self.gdrive_mdata_cleaner(items)
            all_files.extend(clean_mdata)

            self.count_files_crawled += len(items)

            if not next_page_token and not starting:
                print('Time to break... or no files found')
                print(f"Total files processed: {len(all_files)}")
                # TODO: this should return, but should probably enqueue (same as Globus HTTPS crawler)
                # return {"file_mdata": all_files, "crawl_id": self.crawl_id}
                self.pre_commit_time = time.time()
                self.crawl_status = "COMMITTING"
                break

        ta = time.time()
        print("Running grouper...")
        grouped_mdata, tallies = grouper.gen_families(all_files)
        self.crawl_tallies = tallies
        tb = time.time()
        print(f"Tallies: {tallies}")

        self.total_grouping_time += tb - ta

        file_count = 0

        # TODO: Might want to put this above so it happens smoothly DURING processing.
        for item in grouped_mdata:

            self.families_to_enqueue.put({"Id": str(file_count), "MessageBody": json.dumps(item)})
            file_count += 1

        print("SUCCESSFULLY GROUPED METADATA!")

        print(f"Google docs: {self.numdocs}")
        print(f"Regular files: {self.numfiles}")

    def gdrive_mdata_cleaner(self, results_ls):

        new_ls = []
        for res in results_ls:
            if 'webContentLink' not in res:
                res['size'] = 0
                res['is_gdoc'] = True
                self.numdocs += 1
            else:
                self.numfiles += 1
                res['is_gdoc'] = False
                del res['webContentLink']

            shared_peeps = []
            res['user_is_owner'] = False
            if "permissions" in res:
                for person in res['permissions']:
                    # if user and the owner
                    if person['id'] == res['id'] and person['role'] == 'owner':
                        res['user_is_owner'] = True
                    shared_peeps.append(person['id'])
                del res["permissions"]

            res["shared_user_ids"] = shared_peeps

            # TODO: Should this really be a list of all parents?
            if 'parents' in res:
                res["parent"] = res["parents"][0]
                del res["parents"]  # No longer useful to us.
            else:
                res["parent"] = None

            last_mod = res["modifiedTime"]
            last_mod = datetime.datetime.strptime(last_mod, '%Y-%m-%dT%H:%M:%S.%fZ')
            last_mod = last_mod.timestamp()

            res["last_modified"] = last_mod
            del res["modifiedTime"]

            if 'fullFileExtension' in res:
                res['extension'] = res['fullFileExtension']
                del res['fullFileExtension']
            else:
                res['extension'] = ""

            new_ls.append(res)
        return new_ls
