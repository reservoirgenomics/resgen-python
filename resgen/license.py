from pydantic import BaseModel
from typing import Literal
import base64

import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature
import os
from functools import lru_cache
from typing import Optional

PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3gwQsCqz1as9zvbksMFm
1jx8fUDjb5JqPq5ZO6zndxXL8J80Vp8JGmXhpHMXpuWsuV9KgDfxQp4bizzyYvxU
G44GKqEjukDtvOWm1Wy9x/+yRdumAv30Wi/nDgDz/eHdI4i4enlaDW64D3CFdz4n
P0QEI+qB0XYc1torIYiMrlazJg47E0Hr7/5vjcaj8GvWkfm6+6sE7DD4JDfimu4V
PPOovqfVjjajNTKerF7PsU50RkcxwG35+NdXDpgXvrfRWo5I2aDyFYTotLfb2xSh
h0yRRyRRw5sWcJzz+4O0gXZ8ichMFth8E19zglDLYOVYFfaCgQJFU8UM9wY+1Ytu
oQIDAQAB
-----END PUBLIC KEY-----"""


class LicenseInfo(BaseModel):
    permissions: Literal["admin", "guest", "subscription"]
    username: str


class LicenseError(Exception):
    pass

def datasets_allowed(license: LicenseInfo) -> int:
    """Return the number of datasets allowed by the license."""
    if license.permissions == "admin" or license.permissions == 'subscription':
        return 1000000
    elif license.permissions == "guest":
        return 10
    else:
        raise LicenseError("Invalid license permissions")


def b64url_decode(data: str) -> bytes:
    # Add padding if needed
    padding_needed = 4 - (len(data) % 4)
    if padding_needed != 4:
        data += "=" * padding_needed
    return base64.urlsafe_b64decode(data.encode())


def guest_license():
    """Generate a guest license."""
    license_info = LicenseInfo(permissions="guest", username="guest")

    return license_info


def get_license(filepath: Optional[str] = None) -> LicenseInfo:
    """Get the current license. If a filepath is specified, try to load it from there.
    If no filename is specified then try to load from the RESGEN_LICENSE_JWT
    env var. If there's no license there then return a guest license."""
    if filepath:
        with open(filepath, "r") as f:
            license_txt = f.read()

            if not license_txt:
                # Empty license file
                return guest_license()
            
            return license_info(f.read())

    LICENSE_JWT = os.environ.get("RESGEN_LICENSE_JWT")

    if not LICENSE_JWT:
        return guest_license()
    return license_info(LICENSE_JWT)


@lru_cache
def license_info(license_jwt: str):
    """Get the license information from the jwt.

    :param license_jwt: The JWT containing the license info.
    """
    # JWT from earlier
    encoded_header, encoded_payload, encoded_signature = license_jwt.split(".")

    # Rebuild the signing input
    signing_input = f"{encoded_header}.{encoded_payload}".encode()
    signature = b64url_decode(encoded_signature)

    public_key = serialization.load_pem_public_key(PUBLIC_KEY.encode("utf-8"))
    # Verify the signature
    try:
        public_key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
        payload = json.loads(b64url_decode(encoded_payload))

        return LicenseInfo.model_validate(payload)
    except InvalidSignature:
        raise InvalidSignature("Incorrectly signed license")
