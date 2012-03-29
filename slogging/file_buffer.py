import os
import collections
from swift.common.utils import lock_file


class FileBuffer(object):

    def __init__(self, limit, logger):
        self.buffers = collections.defaultdict(list)
        self.limit = limit
        self.logger = logger
        self.total_size = 0

    def write(self, filename, data):
        self.buffers[filename].append(data)
        self.total_size += len(data)
        if self.total_size >= self.limit:
            self.flush()

    def flush(self):
        while self.buffers:
            filename_list = self.buffers.keys()
            for filename in filename_list:
                out = '\n'.join(self.buffers[filename]) + '\n'
                mid_dirs = os.path.dirname(filename)
                try:
                    os.makedirs(mid_dirs)
                except OSError, err:
                    if err.errno == errno.EEXIST:
                        pass
                    else:
                        raise
                try:
                    with lock_file(filename, append=True, unlink=False) as f:
                        f.write(out)
                except LockTimeout:
                    # couldn't write, we'll try again later
                    self.logger.debug(_('Timeout writing to %s' % filename))
                else:
                    del self.buffers[filename]
        self.total_size = 0
