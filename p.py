import requests

url = "https://api-football-v1.p.rapidapi.com/v3/standings"

querystring = {"season":"2020","league":"39"}

headers = {
	"X-RapidAPI-Key": "3367b3e05emsh5518075e6ae2508p1792fdjsna799e84826c0",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json()['response'])