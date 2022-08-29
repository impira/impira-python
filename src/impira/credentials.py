from collections import OrderedDict
import os
from pathlib import Path
from pydantic import BaseModel, validate_arguments
import toml
from typing import Optional


_SDK_HOME = Path(os.environ.get("IMPIRA_SDK_HOME", Path.home() / ".impira"))
CREDENTIALS_PATH = _SDK_HOME / "credentials.toml"
_CREDENTIALS = OrderedDict()
_CREDENTIALS_LOADED = False


class Credentials(BaseModel):
    api_token: str
    org_name: str
    base_url: str

    @classmethod
    def load(cls, api_token=None, org_name=None, base_url=None, **kwargs):
        if api_token is not None and org_name is not None and base_url is not None:
            return Credentials(api_token=api_token, org_name=org_name, base_url=base_url)
        elif api_token is not None and org_name is not None:
            return Credentials(api_token=api_token, org_name=org_name, base_url="https://app.impira.com")
        else:
            return get_credentials(org_name=org_name, base_url=base_url)


def load_credentials(force=False):
    global _CREDENTIALS_LOADED, _CREDENTIALS
    if _CREDENTIALS_LOADED and not force:
        return _CREDENTIALS

    if CREDENTIALS_PATH.exists():
        with open(CREDENTIALS_PATH, "r") as f:
            credentials_toml = toml.load(f)
            all_credentials = [Credentials(**c) for c in credentials_toml["instances"]]
            _CREDENTIALS = OrderedDict([((c.org_name, c.base_url), c) for c in all_credentials])

    _CREDENTIALS_LOADED = True


def get_credentials(org_name=None, base_url=None) -> Optional[Credentials]:
    load_credentials()
    if _CREDENTIALS is not None:
        val = _CREDENTIALS.get((org_name, base_url), None)
        if val is not None:
            return val

        # If there is only one credential matching the unspecified org_name or base_url, return it
        matching = [
            c
            for c in _CREDENTIALS.values()
            if (org_name is None or org_name == c.org_name) and (base_url is None or base_url == c.base_url)
        ]
        if len(matching) == 1:
            return matching[0]

    return None


@validate_arguments
def save_credentials(new_credentials: Credentials):
    credential_values = [x.dict() for x in _CREDENTIALS.values()] + [new_credentials.dict()]
    credentials_toml = {"instances": credential_values}

    _SDK_HOME.mkdir(exist_ok=True)
    with open(CREDENTIALS_PATH, "w") as f:
        toml.dump(credentials_toml, f)
