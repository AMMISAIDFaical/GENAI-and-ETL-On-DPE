import requests

# URL of the API
url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/base-des-diagnostics-de-performance-energetique-dpe-des-batiments-non-residentie/records"
params = {
    "limit": 10,
    "offset": 0,
    "timezone": "UTC",
    "include_links": "false",
    "include_app_metas": "false"
}

if __name__ == "__main__":
    # Headers
    headers = {
        "accept": "application/json; charset=utf-8"
    }

    # Make GET request
    response = requests.get(url, headers=headers, params=params)

    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print(f"Request failed with status code: {response.status_code}")
