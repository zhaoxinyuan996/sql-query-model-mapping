import logging

from flask import g
from sqlalchemy import text
from sqlalchemy.engine.result import RMKeyView
from typing import Tuple, Any, Optional
from contextlib import contextmanager
from flask_sqlalchemy import SQLAlchemy as _SQLAlchemy


log = logging.getLogger(__name__)


class SQLAlchemy(_SQLAlchemy):
    @contextmanager
    def auto_commit(self):
        try:
            yield
            self.session.commit()
            g.db_commit = False
        except Exception as e:
            self.session.rollback()
            raise e

    def execute(self, sql: str, **kwargs) -> Optional[Tuple[RMKeyView, Any]]:
        g.db_commit = True
        log.info(text(sql))
        response = self.session.execute(text(sql), kwargs)
        if response.returns_rows:
            records = response.fetchall()
            log.info(str(records))
            return response.keys(), records
        return None


db = SQLAlchemy()


if __name__ == '__main__':
    ...
