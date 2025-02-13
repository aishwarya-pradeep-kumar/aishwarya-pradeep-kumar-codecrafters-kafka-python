
def check_api_version(request_api_version):
    if 0<= request_api_version <=4:
        return True
    else:
        return False