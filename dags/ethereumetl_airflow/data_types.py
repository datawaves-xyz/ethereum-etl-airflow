from dataclasses import dataclass
from typing import List

from mashumaro import DataClassDictMixin


@dataclass(frozen=True)
class DatabricksClientConfig(DataClassDictMixin):
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_port: int
    databricks_personal_access_token: str
    databricks_mount_name: str
    s3_access_key: str
    s3_access_password: str
    s3_region: str
    s3_bucket: str
    s3_bucket_path_prefix: str

    @property
    def application_args(self) -> List[any]:
        result: List[str] = []

        for key, value in self.to_dict().items():
            result.append('--' + key.replace('_', '-'))
            result.append(value if type(value) == str else str(value))

        return result


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
    def table_name(self) -> str:
        return f'{self.contract_name}_{"call" if self.type == "function" else "evt"}_{self.name}'.lower()

    @property
    def database_name(self) -> str:
        return self.dataset_name.lower()

    @property
    def task_name(self) -> str:
        return f'{self.dataset_name}.{self.contract_name}_{"call" if self.type == "function" else "evt"}_{self.name}'


@dataclass(frozen=True)
class TransferClient(DataClassDictMixin):
    company: str
    abis: List[TransferABI]
    client_config: DatabricksClientConfig

    def dag_name(self) -> str:
        return f'ethereum_transfer_{self.company}'


@dataclass(frozen=True)
class TransferConfig(DataClassDictMixin):
    clients: List[TransferClient]
