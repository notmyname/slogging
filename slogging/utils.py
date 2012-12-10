import hashlib

DISK_CHUNK_SIZE = 65536


def calculate_filehash(fobj):
    """Returns the md5 hexdigest for the contents of a file"""
    filehash = hashlib.md5()
    chunk = fobj.read(DISK_CHUNK_SIZE)
    while chunk:
        filehash.update(chunk)
        chunk = fobj.read(DISK_CHUNK_SIZE)
    return filehash.hexdigest()
