import os
import requests
import logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
import json
from capture.core.utils import get_service
from capture.core.capture import CaptureRunner

URL = get_service("falcoeye-backend")

streaming_user = os.getenv("STREAMING_USER")
streaming_password = os.getenv("STREAMING_PASSWORD")

payload =  {
        "email": streaming_user.strip(),
        "password": streaming_password.strip()
}
logging.info(f"Logging in {URL}")
r = requests.post(f"{URL}/auth/login", json=payload)

assert "access_token" in r.json()
access_token = r.json()["access_token"]
os.environ["JWT_KEY"] = f'JWT {access_token}'


capture_file = os.getenv("CAPTURE_PATH")
with open(capture_file) as f:
    data = json.load(f)

CaptureRunner.run_from_dict(data)
logging.info(f"Capture completed")

