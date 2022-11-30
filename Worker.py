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

        def exposed_put(self, block_uuid, data, worker):
            LOG.info("sent 1 " + str(block_uuid) + str(worker))
            with open(DATA_DIR+str(block_uuid), 'w') as f:
                f.write(data)

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR+str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def exposed_execute_mapred(self, block_uuid, worker, mapper, reducer):
            self.execute_map(block_uuid, worker, mapper)
            LOG.info("Mapper Job Done")
            # self.execute_reduce(block_uuid, worker, reducer)

        def execute_map(self, block_uuid, worker, mapper):
            data = self.exposed_get(block_uuid)
            map_res = subprocess.run(['echo', data, '|', 'python', mapper], shell=True)

        def execute_reduce(self, block_uuid, worker, reducer):
            subprocess.run(['|', 'python', reducer])


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(WorkerService, port=8888)
    t.start()
