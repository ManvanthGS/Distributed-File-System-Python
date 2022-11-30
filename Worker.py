import rpyc
import os
import logging
import subprocess

from rpyc.utils.server import ThreadedServer

DATA_DIR = "D:\pes\sem5\BD\Project\YAMR"

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


class WorkerService(rpyc.Service):
    class exposed_Worker():
        blocks = {}

        def exposed_put(self, block_uuid, data, workers):
            LOG.info("sent 1 " + str(block_uuid) + str(workers))
            with open(DATA_DIR+str(block_uuid), 'w') as f:
                f.write(data)

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR+str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def exposed_execute_mapred(self, data, block_uuid, workers):
            self.execute_map(data, block_uuid, workers)
            self.execute_reduce(data, block_uuid, workers)

        def execute_map(self, data, block_uuid, workers):
            pass

        def execute_reduce(self, data, block_uuid, workers):
            pass


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(WorkerService, port=8888)
    t.start()
