from dataclasses import dataclass
from typing import List

from mashumaro import DataClassDictMixin


@dataclass(frozen=True)
class TransferABI(DataClassDictMixin):
    contract_name: str
    dataset_name: str
    name: str
    type: str

    @property
    def dag_name(self) -> str:
        return f'ethereum_parse_{self.dataset_name}_dag'

    @property
    def task_name(self) -> str:
        return f'{self.dataset_name}.{self.contract_name}_{"call" if self.type == "function" else "evt"}_{self.name}'


@dataclass(frozen=True)
class TransferClient(DataClassDictMixin):
    company: str
    abis: List['TransferABI']

    def dag_name(self) -> str:
        return f'ethereum_transfer_{self.company}'


@dataclass(frozen=True)
class TransferConfig(DataClassDictMixin):
    clients: List[TransferClient]
