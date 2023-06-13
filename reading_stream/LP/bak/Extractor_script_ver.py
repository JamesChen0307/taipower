import gzip
import struct

flowFile = session.get()
if flowFile is not None:
    file_name = flowFile.getAttribute("file_name")

    def read_gz_info(file_name):
        """Reading headers of GzipFile and returning fname."""
        _gz = gzip.GzipFile(file_name)
        _fp = _gz.fileobj

        fname = None
        method = None
        flag = None
        last_mtime = None

        # the magic 2 bytes: if 0x1f 0x8b (037 213 in octal)
        magic = _fp.read(2)
        if magic == b"":
            return None

        if magic != b"\037\213":
            raise OSError("Not a gzipped file (%r)" % magic)

        (method, flag, last_mtime) = struct.unpack("<BBIxx", _read_exact(_fp, 8))
        if method != 8:
            raise OSError("Unknown compression method")

        _fname = []  # bytes for fname
        if flag & gzip.FNAME:
            # Read a null-terminated string containing the filename
            # RFC 1952 <https://tools.ietf.org/html/rfc1952>
            #    specifies FNAME is encoded in latin1
            while True:
                s = _fp.read(1)
                if not s or s == b"\000":
                    break
                _fname.append(s)
            fname = "".join([s.decode("latin1") for s in _fname])

            return fname

    def _read_exact(fp, n):
        """This is the gzip.GzipFile._read_exact() method from the
        Python library.
        """
        data = fp.read(n)
        while len(data) < n:
            b = fp.read(n - len(data))
            if not b:
                raise EOFError(
                    "Compressed file ended before the "
                    "end-of-stream marker was reached"
                )
            data += b
        return data

    try:
        gz_info = read_gz_info(file_name)
        flowFile = session.putAttribute(flowFile, "raw_file", gz_info)
        session.transfer(flowFile, REL_SUCCESS)
    except Exception as e:
        session.transfer(flowFile, REL_FAILURE)
