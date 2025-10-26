import re
import json
import requests
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = "https://www.bcv.org.ve/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9",
}

def extract_rate(html, code):
    pattern = rf'<span>\s*{code}\s*</span>\s*</div>\s*<div[^>]*centrado[^>]*>\s*<strong>\s*([\d,]+)'
    match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
    if match:
        return float(match.group(1).replace(".", "").replace(",", "."))
    return None

def extract_fecha_valor(html):
    match = re.search(r'<span[^>]*class=["\']date-display-single["\'][^>]*>([^<]+)</span>', html, re.IGNORECASE)
    return match.group(1).strip() if match else None

def main():
    try:
        res = requests.get(URL, headers=HEADERS, timeout=10, verify=False)
        res.raise_for_status()
        html = res.text

        data = {
            "success": True,
            "source": "BCV",
            "rates": {
                "USD": extract_rate(html, "USD"),
                "EUR": extract_rate(html, "EUR"),
            },
            "fecha_valor": extract_fecha_valor(html),
            "updated_at": datetime.utcnow().isoformat() + "Z"
        }

        with open("bcv_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("✅ Datos actualizados correctamente")

    except Exception as e:
        print(f"❌ Error: {e}")
        with open("bcv_data.json", "w", encoding="utf-8") as f:
            json.dump({
                "success": False,
                "error": "BCV no disponible",
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
