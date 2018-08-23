import requests
import polling

f = "http://data.gdeltproject.org/gdeltv2/20180813060000.export.CSV.zip"

def chkfileavail(f):
    print("Waiting for File ", f, " to be available.")
    polling.poll(lambda: requests.get(f).status_code == 200, step=60, poll_forever=True)
    print("  >> File Available Now.")

chkfileavail(f)
