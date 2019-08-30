from hdfs import InsecureClient


class HDFSDB(object):
    def __init__(self, http_host, root="/", timeout=10000):
        self.client = InsecureClient(http_host, root=root, timeout=timeout)

    def read_data(self, path, call_back):
        file_list = []
        for parent, dirnames, filenames in self.client.walk(path):
            for file in filenames:
                if file.startswith('part'):
                    print("%s/%s"%(parent,file))
                    file_list.append("%s/%s" % (parent, file))

        for file in file_list:
            with self.client.read(file) as fs:
                lines = fs.read().decode().split('\n')
                for line in lines:
                    if len(line) < 2:
                        continue
                    call_back(line)
