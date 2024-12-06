from uuid import UUID
from flask import Flask
from sql_mapping.base_dao import Dao


class TestDao(Dao):

    def query(self, *_) -> UUID:
        sql = "select id from xxx where xx=x"
        return self.execute(sql)


test_dao = TestDao()

if __name__ == '__main__':
    placeholder_app = Flask(__name__)
    placeholder_app.config['SQLALCHEMY_DATABASE_URI'] = 'xxx'
    with placeholder_app.app_context():
        print(test_dao.query())
