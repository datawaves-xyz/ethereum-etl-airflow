import base64
from datetime import datetime
from typing import Union, Dict, List, Optional

import requests
import yaml


class Token:
    def __init__(self,
                 name: str,
                 id: str,
                 symbol: str,
                 decimals: int,
                 address: Union[int, str],
                 end: Optional[datetime] = None):
        self.name = name
        self.id = id
        self.decimals = decimals
        self.symbol = symbol
        self.end = end

        if address is not None:
            if isinstance(address, str):
                self.address = address
            elif isinstance(address, int):
                self.address = hex(address)
            else:
                raise ValueError('The type of address must be str or int.')

    @classmethod
    def from_dict(cls, dict: Dict[str, any]) -> 'Token':
        return cls(**{k: v for k, v in dict.items() if k in ['name', 'id', 'decimals', 'symbol', 'address', 'end']})


class TokenProvider:
    def get_tokens(self) -> List[Token]:
        raise NotImplementedError()


class DuneTokenProvider(TokenProvider):

    def get_tokens(self) -> List[Token]:
        yaml_file_path = "https://api.github.com/repos/duneanalytics/abstractions/contents/prices/ethereum/coinpaprika.yaml"
        res = requests.get(yaml_file_path)
        yaml_content = base64.b64decode(res.json()['content']).decode('utf-8')

        tokens = []
        for item in yaml.safe_load(yaml_content):
            try:
                tokens.append(Token.from_dict(item))
            except Exception:
                print(f'The item dose not contain ever attribute: {item}')

        return tokens
