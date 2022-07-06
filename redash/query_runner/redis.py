import redis
from cmath import inf, log
from redash.query_runner import TYPE_STRING, BaseQueryRunner, register, TYPE_INTEGER

import logging
from redash.query_runner.json_ds import QueryParseError

from redash.utils import json_dumps
logger = logging.getLogger(__name__)
try:

    enabled = True

except ImportError:
    enabled = False

# PRESTO_TYPES_MAPPING = {
#     "integer": TYPE_INTEGER,
#     "tinyint": TYPE_INTEGER,
#     "smallint": TYPE_INTEGER,
#     "long": TYPE_INTEGER,
#     "bigint": TYPE_INTEGER,
#     "float": TYPE_FLOAT,
#     "double": TYPE_FLOAT,
#     "boolean": TYPE_BOOLEAN,
#     "string": TYPE_STRING,
#     "varchar": TYPE_STRING,
#     "date": TYPE_DATE,
# }


class Redis(BaseQueryRunner):
    noop_query = "SELECT 1"
    should_annotate_query = False
    # redis_pool = None

    @classmethod
    def type(cls):
        return "redis"

    @classmethod
    def name(cls):
        return "Redis"

    def run_query(self, query, user):
        # logger.info(f'Running query {query} by user: {user}')
        # host = self.configuration.get("host", "localhost")
        # user = self.configuration.get("user", "user")
        # password = self.configuration.get("password", "password")
        # db = self.configuration.get("db", 0)
        # port = self.configuration.get("port", 3306)
        # import redis
        # logger.info(f'Starting redis connection for host={host}, port={port}, db={db}')
        # r = redis.Redis(host=host, port=port, password=password)
        return self._parse_execute_query(query, self._get_connection())

    def _parse_execute_query(self, line, r: redis.Redis):
        import re
        args = re.split(r"\s+", line)
        if len(args) < 1:
            raise QueryParseError("Query must include data structure name.")
        if len(args) < 2:
            raise QueryParseError("Query must include key name.")

        ds = args[0].lower()
        name = args[1].lower()

        if ds == 'key':
            val = r.get(name)
            if not val:
                raise QueryParseError("Key doesnot exist")
            return self._parse_key(self.decode_binary(val))
        elif ds == 'zset':
            return self._parse_zset(r.zrangebyscore(name, -inf, +inf))
        elif ds == 'hashkey':
            if not args[2]:
                raise QueryParseError("Query must include key name for hashkey data structures.")
            key = args[2]
            return r.hget(name, key)
        elif ds == 'hash':
            return self._parse_hash(self.byte_list_to_str_list(r.hgetall(name)))
        else:
            raise QueryParseError(f"Query not supported for {ds} data structures.")

    def decode_binary(self, b):
        try:
            return b.decode('utf-8')
        except:
            logger.error(f'problem in decoding binary value {b}')
            return b

    def byte_list_to_str_list(self, byte_dict):
        non_byte_dict = {}
        if byte_dict:
            for k, v in byte_dict.items():
                try:
                    non_byte_dict[k.decode('utf-8')] = str(v.decode('utf-8'))
                except:
                    logger.info(f'problem in decoding key {k} or value {v}')
        return non_byte_dict

    def _parse_key(self, data):
        def columns():
            return [
                {
                    "name": "value",
                    "friendly_name": "Value",
                    "type": TYPE_STRING,
                }
            ]

        result = {
            "rows": [
                {
                    "value": str(data)
                },
            ],
            "columns": columns()
        }
        logger.info(result)
        return json_dumps(result), None

    def _parse_zset(self, data):
        def columns():
            return [
                {
                    "name": "member",
                    "friendly_name": "Member",
                    "type": TYPE_STRING,
                }
            ]

        def Merge(dict1, dict2):
            return(dict2.update(dict1))
        row_list = []
        for k in data:
            row_list.append({
                'member': str(k.decode('utf-8'))
            })
        result = {
            "rows": row_list,
            "columns": columns()
        }
        logger.info(result)
        return json_dumps(result), None

    def _parse_hash(self, data):
        def columns():
            return [
                {
                    "name": "key",
                    "friendly_name": "Key",
                    "type": TYPE_STRING,
                },
                {
                    "name": "value",
                    "friendly_name": "Value",
                    "type": TYPE_STRING,
                }
            ]
        # Python code to merge dict using update() method

        def Merge(dict1, dict2):
            return(dict2.update(dict1))
        row_list = [{}]
        for k, v in data.items():
            row_list.append({
                'key': k,
                'value': v
            })
        result = {
            "rows": row_list,
            "columns": columns()
        }
        logger.info(result)
        return json_dumps(result), None

    def get_schema(self, get_stats=False):
        conn = self._get_connection()
        keys = conn.keys()
        schema = {}
        for k in keys:
            try:
                key = k.decode('utf-8')
                if key not in schema:
                    type = conn.type(key).decode('utf-8')
                    schema[key] = {"name": key, "columns": [type + ' ' + key]}
            except:
                logger.error(f'problem in getting type for key {k}')
        logger.info(f'schema is {schema}')
        return schema

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "number"},
                "useSSL": {"type": "boolean"},
                "username": {"type": "string"},
                "password": {"type": "string"},
                "db": {"type": "string"},
            },
            "order": [
                "host",
                "port",
                "username",
                "password",
                "db",
                "useSSL",
            ],
            "required": ["host"],
        }

    def test_connection(self):
        self._get_connection().keys()

    def _get_connection(self) -> redis.Redis:
        # global redis_pool
        host = self.configuration.get("host", "localhost")
        password = self.configuration.get("password", "password")
        db = self.configuration.get("db", 0)
        port = self.configuration.get("port", 3306)
        useSSL = self.configuration.get("useSSL", False)
        logger.info(f'Starting redis connection for host={host}, port={port}, db={db}, ssl={useSSL}')
        # redis_pool = redis.ConnectionPool(host=host, port=port, db=db, password=password, ssl= useSSL)
        redis_conn = redis.Redis(host=host, port=port, db=db, password=password, ssl=useSSL)
        return redis_conn

    @classmethod
    def enabled(cls):
        return enabled


register(Redis)
