import requests

urls = [
    "https://freekaamaal.com/search?keyword=myntra",
    "https://www.snapdeal.com/search?keyword=myntra",
    "https://www.jiomart.com/search/myntra"
]

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

for url in urls:
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"URL: {url} -> Status: {r.status_code}")
        print(f"Content length: {len(r.text)}")
        if r.status_code == 200:
            if "captcha" in r.text.lower() or "cloudflare" in r.text.lower() or "access denied" in r.text.lower():
                print("Looks like CAPTCHA/Cloudflare block.")
    except Exception as e:
        print(f"URL: {url} -> Error: {e}")
    print("-" * 40)
