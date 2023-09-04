# docker run --name postgis-sandbox -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgis/postgis
# psql -h 127.0.0.1 -U postgres -d postgres -p 5432
import logging
from typing import Union

# from sqlalchemy.orm import sessionmaker

from datetime import datetime
from sqlalchemy import func, create_engine
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import Engine
from pydantic import BaseModel


# pydantic
class Postgres(BaseModel):
    dbname: str = "postgres"
    user: str = "postgres"
    password: str = "mysecretpassword"
    host: str = "127.0.0.1"
    port: str = "5432"

    @property
    def uri(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    @property
    def engine(self) -> Engine:
        return create_engine(self.uri)


# SQLAlchemy2 Declarative Class
class Base(DeclarativeBase):
    pass


class Log(Base):
    __tablename__ = "logs"
    __table_args__ = {"schema": "logs"}
    id: Mapped[int] = mapped_column(primary_key=True)
    logger_name: Mapped[str] = mapped_column(String(30))
    level_name: Mapped[str] = mapped_column(String(30))
    message: Mapped[str] = mapped_column(String(150))
    created_at: Mapped[datetime] = mapped_column(insert_default=func.now())

    def __repr__(self):
        return f"""Log(id={self.id!r}, logger_name={self.logger_name!r}, level_name={self.level_name!r}, message={self.message!r}, created_at={self.created_at!r})"""


class PostgresHandler(logging.StreamHandler):
    def __init__(self, config: Union[dict, None] = None):
        super().__init__()
        self.engine = Postgres(**(config or {})).engine
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()

        # create log table if not exits
        Base.metadata.create_all(self.engine)

    def emit(self, record):
        log_entry = self.format(record)
        try:
            self.conn.execute(
                Log.__table__.insert(),
                [
                    {
                        "logger_name": record.name,
                        "level_name": record.levelname,
                        "message": log_entry,
                    },
                ],
            )
            self.trans.commit()
        except Exception as e:
            self.trans.rollback()
            self.handleError(record)
            raise f"Could not write log: {e}"

    def close(self):
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()


class Logger:
    def __init__(self, module_name: str):
        self.logger = logging.getLogger(module_name)

        # Replace FileHandler with PostgresHandler
        self.db_handler = PostgresHandler()

        # Formatter remains the same
        self.db_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.db_handler.setFormatter(self.db_format)

        self.logger.addHandler(self.db_handler)
        self.logger.setLevel(logging.DEBUG)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)
