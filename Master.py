import rpyc
import uuid
import math
import random
import socket

from rpyc.utils.server import ThreadedServer


class MasterService(rpyc.Service):
    class exposed_Master():
        file_table = {}
        block_mapping = {}
        workers = {}

        block_size = 0
        replication_factor = 2
        number_of_workers = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self, dest, size):
            if self.exists(dest):
                pass

            self.__class__.file_table[dest] = []

            self.set_block_size(size)
            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            else:
                return None

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_workers(self):
            return self.__class__.workers

        def exposed_set_number_of_workers(self, W):
            self.__class__.number_of_workers = W
            host = socket.gethostbyname(socket.gethostname())
            for i in range(1, W+1):
                self.__class__.workers[i] = (host, 8887 + i)

        def set_block_size(self, size):
            W = self.__class__.number_of_workers
            s = int(math.ceil(float(size)/W))
            self.__class__.block_size = s

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size)/self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_ids = random.sample(
                    self.__class__.workers.keys(), self.__class__.replication_factor)
                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest].append((block_uuid, nodes_ids))

            return blocks


if __name__ == "__main__":
    t = ThreadedServer(MasterService, port=2100)
    t.start()
