import base64
import json
import os
import struct
import socket
import time
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse


def pawsey_stage_files(file_list, host, port):
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
            time.sleep(10)
            exitcode = -95

        return exitcode

    except Exception as e:
        raise Exception(f"Other error staging data: {str(e)}")

    finally:
        if sock:
            sock.close()


def pawsey_discard(host: str, disk_id: str, file_id: str, file_version: int, obsid: int, execute: bool):
    #
    # Discard (delete) the ngas file. The ngas server will delete the row from the ngas database and remove the
    # file from the pawsey filesystem
    #
    url = str(host).replace(":", ".pawsey.org.au:")

    httpstatus, statusreason, data = _discard_file(url, disk_id, file_id, file_version, execute)

    if httpstatus != 200:
        raise Exception("Result: {0} {1} {2}".format(httpstatus, statusreason, data))


def _discard_file(url, ngas_user, ngas_pass, disk_id, file_id, file_version, execute):
    query_args = {'disk_id': disk_id,
                  'file_id': file_id,
                  'file_version': file_version,
                  'execute': execute, }

    url = 'http://{0}/{1}?{2}'.format(url, 'DISCARD', urllib.parse.urlencode(query_args))

    resp = None
    data = []

    try:
        request = urllib.request.Request(url)

        credentials = ('%s:%s' % (ngas_user, ngas_pass))
        encoded_credentials = base64.b64encode(credentials.encode('ascii'))
        request.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii"))

        resp = urllib.request.urlopen(request, timeout=7200)

        while True:
            buff = resp.read().decode("utf-8")
            if not buff:
                break
            data.append(buff)

        status = "".join(data)

        if 'Status="SUCCESS"' in status:
            return 200, '', ''
        else:
            raise Exception("Error status executing DISCARD: {0}", status)

    except Exception as discard_exception:
        raise Exception("Exception executing DISCARD: {0} {1} {2}", url, data, discard_exception)

    finally:
        if resp:
            resp.close()