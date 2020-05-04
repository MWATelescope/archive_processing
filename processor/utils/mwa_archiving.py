import json
import os
import struct
import socket
import time


def pawsey_stage_files(file_list: list, host: str, port: int):
    simulate_failure = False
    sock = None

    try:
        # Filelist should be just filenames no paths
        # If input is list of filenames that's ok
        # If input is list of full paths that's ok
        json_output = json.dumps({'files': [os.path.split(f)[1] for f in file_list]})

        output = bytearray()
        output.extend(struct.pack('>I', len(json_output)))
        output.extend(json_output.encode())

        if not simulate_failure:
            # The real deal
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(output)
            sock.settimeout(4500)

            exitcode = struct.unpack('!H', sock.recv(2))[0]
        else:
            # Simulate Failure
            time.sleep(2)
            exitcode = -95

        return exitcode

    except Exception as e:
        raise Exception(f"Other error staging data: {str(e)}")

    finally:
        if sock:
            sock.close()
