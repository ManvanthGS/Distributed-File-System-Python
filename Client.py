import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


def send_to_worker(block_uuid, data, workers):
    LOG.info("sending: " + str(block_uuid) + str(workers))
    worker = workers[0]
    workers = workers[1:]
    host, port = worker

    con = rpyc.connect(host, port=port)
    worker = con.root.Worker()
    worker.put(block_uuid, data, workers)


def read_from_worker(block_uuid, worker):
    host, port = worker
    con = rpyc.connect(host, port=port)
    worker = con.root.Worker()
    return worker.get(block_uuid)


def get(master, fname):
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        for m in [master.get_workers()[_] for _ in block[1]]:
            data = read_from_worker(block[0], m)
            if data:
                sys.stdout.write(data)
                break
        else:
            LOG.info("No blocks found. Possibly a corrupt file")


def put(master, source, dest):
    size = os.path.getsize(source)
    blocks = master.write(dest, size)
    with open(source) as f:
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid = b[0]
            workers = [master.get_workers()[_] for _ in b[1]]
            send_to_worker(block_uuid, data, workers)


def main(args, W):
    con = rpyc.connect("localhost", port=2100)
    master = con.root.Master()

    master.set_number_of_workers(W)

    if args[0] == "get":
        get(master, args[1])
    elif args[0] == "put":
        put(master, args[1], args[2])
    else:
        LOG.error("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    number_of_workers = 3
    main(sys.argv[1:],number_of_workers)
