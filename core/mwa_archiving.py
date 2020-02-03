import asyncio
import json
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
import struct


class StagingError(Exception):
    pass


@retry(stop=stop_after_attempt(4), wait=wait_fixed(30), retry=retry_if_exception_type(StagingError), reraise=True)
async def pawsey_stage_files(file_list, host, port):
    writer = None
    try:
        json_output = json.dumps({'files': file_list})

        output = bytearray()
        output.extend(struct.pack('>I', len(json_output)))
        output.extend(json_output.encode())
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(output)
        await writer.drain()
        response = await reader.readexactly(2)
        exitcode = struct.unpack('!H', response)[0]
        if exitcode != 0:
            raise StagingError(f"Error staging data: {str(exitcode)}")
        return exitcode
    except asyncio.CancelledError:
        raise
    except StagingError:
        raise
    except Exception as e:
        raise StagingError(f"Error staging data: {str(e)}")
    finally:
        if writer:
            writer.close()

