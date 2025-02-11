import struct

def check_api_version(request_api_version):
    request_api_key = struct.unpack(">h", request_api_version[6:8])[0]
    print(f'request_api_key: {request_api_key}')
    if 0<= request_api_key <=4:
        return True
    else:
        return False