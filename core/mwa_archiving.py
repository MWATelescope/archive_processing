import json
import os
import struct
import socket
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


class StagingError(Exception):
    pass


@retry(stop=stop_after_attempt(4), wait=wait_fixed(30), retry=retry_if_exception_type(StagingError), reraise=True)
def pawsey_stage_files(file_list, host, port):
    sock = None
    try:
        # Filelist should be just filenames no paths
        # If input is list of filenames that's ok
        # If input is list of full paths that's ok
        json_output = json.dumps({'files': [os.path.split(f)[1] for f in file_list]})

        #val = struct.pack('>I', len(json_output))
        #val = val + json_output

        output = bytearray()
        output.extend(struct.pack('>I', len(json_output)))
        output.extend(json_output.encode())

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(output)
        sock.settimeout(4500)

        exitcode = struct.unpack('!H', sock.recv(2))[0]

        if exitcode != 0:
            raise StagingError(f"Error staging data: {str(exitcode)}")

        return exitcode

    except StagingError:
        raise

    except Exception as e:
        raise StagingError(f"Error staging data: {str(e)}")

    finally:
        if sock:
            sock.close()

