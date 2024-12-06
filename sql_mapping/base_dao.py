from datetime import datetime
from functools import partial
from typing import Callable, Any, Union, get_origin, get_args
from uuid import UUID
from sqlalchemy import Row
from .database import db


_ele = {bool, int, float, str, datetime, UUID, Any}
_builtins = {dict, list, set, int, float, str, datetime}


def build_model(t, keys, struct) -> Any:
    """
    sql返回结果结合pydantic
    :param t: 类型
    :param keys: 读数据库时候的key
    :param struct: 数据体
    """
    if type_ := get_origin(t):

        if type_ in _builtins:
            assert type_ is type(struct)
        if type_ is list:
            return [build_model(get_args(t)[0], keys, i) for i in struct]
        elif type_ is tuple:
            return struct[0]
        elif type_ is dict:
            return t(**struct)
        elif type_ is int or type_ is float or type_ is str:
            return struct
        elif type_ is Union:
            if type(None) in get_args(t):
                if not struct:
                    return None
                else:
                    return build_model(get_args(t), keys, struct)
            else:
                return build_model(get_args(t), keys, struct)
    if keys is None:
        return t(**struct)
    if t in _ele:
        if isinstance(struct, Row):
            return struct[0]
        return struct[0][0]
    return t(**dict(zip(keys, struct[0] if isinstance(struct, list) else struct)))


def wrap(self, f: Callable, *args, **kwargs) -> Any:
    """装饰器，如果dao方法声明了返回值，则按照返回值格式化"""
    if ret := getattr(f, '__annotations__').get('return', None):
        resp = f(self, *args, **kwargs)
        model, entry = resp
        return build_model(ret, model, entry)
    f(self, *args, **kwargs)
    return None


class Dao:

    def __init__(self):
        for k, v in type(self).__dict__.items():
            if not k.startswith('__') and isinstance(v, Callable):
                setattr(self, k, partial(wrap, self, getattr(type(self), k)))

    @staticmethod
    def execute(sql: str, **kwargs) -> Any:
        return db.execute(sql, **kwargs)

    @staticmethod
    def text(sql: str, **kwargs) -> str:
        """打印下sql debug用"""
        for k, v in kwargs.items():
            sql = sql.replace(f':{k}', f"'{v}'" if isinstance(v, (str, UUID)) else str(v))
        return sql


base_dao: Dao = Dao()
