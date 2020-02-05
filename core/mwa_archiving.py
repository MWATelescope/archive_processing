import json
import struct
import socket
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


class StagingError(Exception):
    pass


@retry(stop=stop_after_attempt(4), wait=wait_fixed(30), retry=retry_if_exception_type(StagingError), reraise=True)
def pawsey_stage_files(file_list, host, port):
    sock = None
    try:
        json_output = json.dumps({'files': file_list})

        val = struct.pack('>I', len(json_output))
        val = val + json_output

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(val)
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

