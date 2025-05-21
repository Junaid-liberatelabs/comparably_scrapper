from curl_cffi.requests import request

def curl_get(url):
    """
    Perform a GET request using curl_cffi.
    """
    response = request(url)
    return response
res = curl_get("https://api.github.com")
print(res)
print(res.status_code)