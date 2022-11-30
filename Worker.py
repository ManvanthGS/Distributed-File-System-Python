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
            if len(workers) > 0:
                self.forward(block_uuid, data, workers)

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR+str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def forward(self, block_uuid, data, workers):
            LOG.info("sent 2 " + str(block_uuid) + str(workers))
            worker = workers[0]
            workers = workers[1:]
            host, port = worker

            con = rpyc.connect(host, port=port)
            LOG.info("connection established for next node")
            worker = con.root.Worker()
            worker.put(block_uuid, data, workers)


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(WorkerService, port=8890)
    t.start()