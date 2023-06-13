import os
import sys
import redis
from os.path import dirname

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))
from ami import conn
from dataclasses import dataclass, field


@dataclass
class RedisEngine:
    HOST: str = field(default=conn.MDES_REDIS_HOST)
    PORT: int = field(default=conn.MDES_REDIS_PORT)
    PWD: str = field(default=conn.MDES_REDIS_PASS)
    DB: int = field(default=conn.MDES_REDIS_DB)

    def __post_init__(self):
        self.domain = self.HOST + self.PORT
        self.client = None

    def get_client(self):
        if self.client:
            return self.client

        self.client = redis.ConnectionPool(
            host=self.HOST,
            port=self.PORT,
            password=self.PWD,
            db=self.DB,
        )
        return self.client


red1 = RedisEngine(DB=123)
red2 = RedisEngine(DB=456)
red1.get_client()
