

import time
from psycopg2.extras import Json, DictCursor
from utils import pg_utils as pg


class Crawler:
    def __init__(self):
        self.init_time = time.time()
        self.end_time = None

    def push_results_to_queue(self):
        print("PUSHING TO QUEUE")

    # TODO: Can I erase these?
    # def pg_setup(self):
    #     self.conn = pg.pg_conn()
    #     self.cur = self.conn.cur()
    #
    # def push_entry_to_pg(self, cur, f_path, f_size, f_ext, f_cur_phase, f_metadata,
    #                      f_created_on, f_last_extracted, f_owner, f_crawl_type):
    #     update_string = f"""INSERT INTO files (path, size, extension, cur_phase,
    #                        metadata, created_on, last_extracted, owner, crawl_type)
    #                        VALUES ('{f_path}', {f_size}, '{f_ext}', '{f_cur_phase}', {Json(
    #         f_metadata)}, '{f_created_on}', '{f_last_extracted}', '{f_owner}', '{f_crawl_type}');"""
    #
    #     pg.pg_update(cur, update_string)

    # TODO: This should be overridden (e.g., this should be used as an interface).
    # def crawl(self):
    #     pass



