# import curl_cffi

# # Notice the impersonate parameter
# r = curl_cffi.get("https://www.comparably.com/companies/google/reviews?_=companyReviews_85~N4IgDghg5gpgzgSwF4xALgEwAYA0IEB2AJjAB7rZ4AuA9lRADYUDMAbHhAE5RzqgCOAV3hUENAgEki6ABwBWPAGMIVGFBqcAngAUVAC3QgQSlWo2aAMhABGMJmiNKaAW0gEd%2Bw%2BppQGqJ64Q7lIsAJxY7CAkkJxUzjAEVADKDIJQ6ASCDAx40VxxCVQhaLggolR%2BxaXlfilpGVk5IHCFCPGJhoxMeABmCAyqnADCpupahkMuYAlwKmIEIAC%2Bi0A%3D%40companyReviews_95~N4IgDghg5gpgzgSwF4xALgEwAYA0IEB2AJjAB7q4gAuA9lRADboDMAnHhAE5RzqgCOAV3hUENAgEki6VgFY8AYwhUYUGpwCeABWUALdCBCLlq9RoAyEAEYwmaQ4poBbSAW16DamlAapHLiDcpdAw2LAA2PBJITionGAIqAGUGQSh0AkEGBiiYGLiEqmC0SlEqX2LShHKYFLSMrJyQOEKEeMSDRiY8ADMEBhVOAGETNU0DIecwBLhlMQIQAF9FoA%3D", impersonate="chrome")

# with open("browserleaks.html", "w",encoding="utf-8") as f:
#     f.write(r.text)
    
    

from seleniumwire import webdriver

def test_capture_req():
    driver = webdriver.Chrome()
    driver.get("https://www.comparably.com/companies/google/reviews")
    
    for request in driver.requests:
        if request.response:
            print(
                request.url,
                request.response.status_code,
                request.response.headers['Content-Type']
            )
test_capture_req()