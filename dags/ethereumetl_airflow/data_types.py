from dataclasses import dataclass
from typing import List

from mashumaro import DataClassDictMixin


@dataclass(frozen=True)
class DatabricksClientConfig(DataClassDictMixin):
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_port: int
    databricks_personal_access_token: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    s3_bucket_path_prefix: str
    schema_registry_s3_access_key: str
    schema_registry_s3_secret_key: str
    schema_registry_s3_region: str
    load_all: str

    @property
    def application_args(self) -> List[any]:
        result: List[str] = []

        for key, value in self.to_dict().items():
            result.append('--' + key.replace('_', '-'))
            result.append(value if type(value) == str else str(value))

        return result


@dataclass(frozen=True)
class TransferABI(DataClassDictMixin):
    group_name: str
    contract_name: str
    abi_name: str
    abi_type: str

    @property
    def upstream_dag_name(self) -> str:
        return f'ethereum_parse_{self.group_name}_dag'

    @property
    def task_name(self) -> str:
        return f'{self.group_name}.{self.contract_name}_{"call" if self.abi_type == "function" else "evt"}_{self.abi_name}'


@dataclass(frozen=True)
class TransferClient(DataClassDictMixin):
    company: str
    raws: List[str]
    abis: List[TransferABI]
    client_config: DatabricksClientConfig

    @property
    def dag_name(self) -> str:
        return f'ethereum_transform_{self.company}_dag'

    @property
    def raw_dag_name(self) -> str:
        return f'ethereum_raw_transform_{self.company}_dag'

    @property
    def application_args(self) -> List[any]:
        return self.client_config.application_args


@dataclass(frozen=True)
class TransferConfig(DataClassDictMixin):
    clients: List[TransferClient]
