"""Public configuration and internal caller settings."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

PathLike = Union[str, Path]


def positive(name, value, minimum=1):
    if type(value) is not int or value < minimum:
        raise ValueError(f'{name} must be an integer >= {minimum}')


@dataclass(frozen=True)
class StorageConfig:
    output_path: PathLike = 'results.jsonl'
    save_every_n: int = 50
    resume: bool = True

    def __post_init__(self):
        positive('save_every_n', self.save_every_n)
        if type(self.resume) is not bool:
            raise ValueError('resume must be a bool')


@dataclass(frozen=True)
class RetryConfig:
    max_retries: int = 3

    def __post_init__(self):
        positive('max_retries', self.max_retries, 0)


@dataclass(frozen=True)
class LogConfig:
    log_every_n: int = 100
    console: bool = True
    log_path: Optional[PathLike] = None

    def __post_init__(self):
        positive('log_every_n', self.log_every_n)
        if type(self.console) is not bool:
            raise ValueError('console must be a bool')
