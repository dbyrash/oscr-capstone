import os
import requests
from dotenv import load_dotenv

load_dotenv()

def verify_finnhub_api_key():
    api_key = os.getenv("FINNHUB_API_KEY")

    if not api_key:
        print("FINNHUB_API_KEY is not set")
        return False

    url = "https://finnhub.io/api/v1/quote?symbol=NFLX&token=" + api_key
    response = requests.get(url)
    if response.ok:
        print(response.json())
        print("FINNHUB_API_KEY is valid")
        return True
    else:
        print(f"FINNHUB_API_KEY is invalid: {response.status_code} {response.text}")
        return False

def verify_tmdb_api_key():
    api_key = os.getenv("TMDB_API_KEY")

    if not api_key:
        print("TMDB_API_KEY is not set")
        return False

    url = "https://api.themoviedb.org/3/movie/popular?api_key=" + api_key

    response = requests.get(url)

    if response.ok:
        print(response.json())
        print("TMDB_API_KEY is valid")
        return True
    else:
        print(f"TMDB_API_KEY is invalid: {response.status_code} {response.text}")
        return False

if __name__ == "__main__":
    verify_finnhub_api_key()
    verify_tmdb_api_key()
    print("All API keys are valid")
