import base64
import http.client
import json
import os
import requests
import struct
import socket
import time
from processor.utils.mwa_metadata import MWAFileTypeFlags


class NGASHttpPushConnector(object):

   def __init__(self, url, command, username, password, mimetype):
      self.url = url
      self.command = command
      self.username = username
      self.password = password
      self.mimetype = mimetype

   def transfer_file(self, fullpath):

      filename = os.path.basename(fullpath)
      if not filename:
         raise Exception('could not extract basename from %s' % fullpath)

      filesize = os.stat(fullpath).st_size

      conn = None

      try:
         conn = http.client.HTTPConnection(self.url)

         conn.putrequest("POST", self.command)
         auth_string = f"{self.username}:{self.password}"
         auth_bytes = auth_string.encode("ascii")
         base64_bytes = base64.encodebytes(auth_bytes)
         base64_string = base64_bytes.decode().replace("\n", "")
         conn.putheader("Authorization", "Basic %s" % base64_string)

         conn.putheader("Content-disposition", f"attachment; filename={filename}")
         conn.putheader("Content-length", str(filesize))
         conn.putheader("Host", socket.gethostname())
         conn.putheader("Content-type", self.mimetype)
         conn.endheaders()

         blocksize = 65536
         sent = 0
         with open(fullpath, "rb") as file:
             while True:
                databuff = file.read(blocksize)
                if not databuff:
                    break
                conn.sock.sendall(databuff)
                sent += len(databuff)

         if sent != filesize:
            raise Exception(f"data sent does not match filesize: {str(sent)} != {str(filesize)}")

         resp = conn.getresponse()
         data = []
         while True:
            buff = resp.read()
            if not buff:
               break
            data.append(buff.decode())

         return resp.status, resp.reason, ''.join(data)

      finally:
         if conn:
            conn.close()


def archive_file(database_pool, ngas_host: str, ngas_port: int, ngas_user: str, ngas_pass: str,
                 obs_id: int, full_filename: str, file_type: MWAFileTypeFlags):
    obs_id = str(obs_id)
    filename = os.path.split(full_filename)[1]
    file_size = os.stat(full_filename).st_size
    file_uri = f"http://mwangas/RETRIEVE?file_id={filename}"

    sql = f"INSERT INTO data_files " \
          f"(observation_num, filetype, size, filename, site_path, remote_archived) " \
          f"VALUES (%s, %s, %s, %s, %s, true)"
    sql_params = [str(obs_id), str(file_type.value), str(file_size), filename, file_uri, ]

    cursor = None
    con = None

    try:
        con = database_pool.getconn()
        cursor = con.cursor()
        cursor.execute(sql, sql_params)

    except Exception as db_exception:
        if con:
            con.rollback()
        raise db_exception
    else:
        # The SQL worked- check that we inserted 1 and only 1 row
        rows_affected = cursor.rowcount

        if rows_affected == 1:
            try:
                # Perform the NGAS part of archving
                ngas_url = f"{ngas_host}:{str(ngas_port)}"

                ngas_conn = NGASHttpPushConnector(ngas_url,
                                                  "QARCHIVE",
                                                  ngas_user,
                                                  ngas_pass,
                                                  "application/octet-stream")

                stat, reason, data = ngas_conn.transfer_file(full_filename)

                # http code of 200 means we're good. If not raise exception and roll back the db transaction
                if stat != 200:
                    raise Exception(reason)

            except Exception as ngas_exception:
                if con:
                    con.rollback()
                raise ngas_exception

            if con:
                con.commit()
        else:
            if con:
                con.rollback()
    finally:
        if cursor:
            cursor.close()
        if con:
            database_pool.putconn(conn=con)


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

