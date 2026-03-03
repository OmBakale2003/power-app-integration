from graph import get_req_custom_url

def main():
    additional_headers = {
    "ConsistencyLevel": "eventual",
    "Accept": "text/plain"
    }
    data = get_req_custom_url(append_url="devices/$count", additional_headers= additional_headers)   
    print(data)
   
if __name__ == "__main__":
    main();