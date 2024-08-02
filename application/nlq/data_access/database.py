import logging

import sqlalchemy as db
from sqlalchemy import  Column, inspect
import boto3

from nlq.data_access.dynamo_connection import ConnectConfigEntity

from utils.env_var import REDSHIFT_CLUSTER_IDENTITIER, REDSHIFT_SERVERLESS_WORK_GRP

logger = logging.getLogger(__name__)


class RelationDatabase():
    db_mapping = {
        'mysql': 'mysql+pymysql',
        'postgresql': 'postgresql+psycopg2',
        'protonbase': 'postgresql+psycopg2',
        'redshift': 'postgresql+psycopg2',
        'redshift-iam': 'postgresql+psycopg2',
        'redshift-serverless-iam': 'redshift+psycopg2',
        'starrocks': 'starrocks',
        'clickhouse': 'clickhouse',
        # Add more mappings here for other databases
    }

    @classmethod
    def get_redshift_resp_with_iam(cls, db_type, db_name):
        db_user = None
        db_password = None
        if db_type == 'redshift-iam':
            redshift = boto3.client("redshift")
            resp = redshift.get_cluster_credentials_with_iam(
                DbName=db_name,
                ClusterIdentifier=REDSHIFT_CLUSTER_IDENTITIER,
                DurationSeconds=1000,
            )
            db_user = resp['DbUser']
            db_password = resp['DbPassword']
        if db_type == 'redshift-serverless-iam':
            redshift = boto3.client("redshift-serverless")
            resp = redshift.get_credentials(
                dbName=db_name,
                workgroupName=REDSHIFT_SERVERLESS_WORK_GRP,
                durationSeconds=1000
            )
            db_user = resp['dbUser']
            db_password = resp['dbPassword']

        return db_user, db_password

    @classmethod
    def get_db_url(cls, db_type, user, password, host, port, db_name):
        if db_type in ('redshift-iam', 'redshift-serverless-iam'):
            user, password = cls.get_redshift_resp_with_iam(db_type, db_name)
        db_url = db.engine.URL.create(
            drivername=cls.db_mapping[db_type],
            username=user,
            password=password,
            host=host,
            port=port,
            database=db_name
        )
        return db_url

    @classmethod
    def test_connection(cls, db_type, user, password, host, port, db_name) -> bool:
        try:
            engine = db.create_engine(cls.get_db_url(db_type, user, password, host, port, db_name))
            connection = engine.connect()
            return True
        except Exception as e:
            logger.exception(e)
            logger.error(f"Failed to connect: {str(e)}")
            return False

    @classmethod
    def get_all_schema_names_by_connection(cls, connection: ConnectConfigEntity):
        db_type = connection.db_type
        db_url = cls.get_db_url(db_type, connection.db_user, connection.db_pwd, connection.db_host, connection.db_port,
                                connection.db_name)
        engine = db.create_engine(db_url)
        inspector = inspect(engine)

        if db_type == 'postgresql':
            schemas = [schema for schema in inspector.get_schema_names() if
                       schema not in ('pg_catalog', 'information_schema')]
        elif db_type in (
        'protonbase', 'redshift', 'redshift-iam', 'redshift-serverless-iam', 'mysql', 'starrocks', 'clickhouse'):
            schemas = inspector.get_schema_names()
        else:
            raise ValueError("Unsupported database type")

        return schemas

    @classmethod
    def get_all_tables_by_connection(cls, connection: ConnectConfigEntity, schemas=None):
        if schemas is None:
            schemas = []
        metadata = cls.get_metadata_by_connection(connection, schemas)
        return metadata.tables.keys()

    @classmethod
    def get_metadata_by_connection(cls, connection, schemas):
        db_url = cls.get_db_url(connection.db_type, connection.db_user, connection.db_pwd, connection.db_host,
                                connection.db_port, connection.db_name)
        engine = db.create_engine(db_url)
        # connection = engine.connect()
        metadata = db.MetaData()
        for s in schemas:
            metadata.reflect(bind=engine, schema=s, views=True)
        # metadata.reflect(bind=engine)
        return metadata

    @classmethod
    def get_table_definition_by_connection(cls, connection: ConnectConfigEntity, schemas, table_names):
        metadata = cls.get_metadata_by_connection(connection, schemas)
        tables = metadata.tables
        table_info = {}

        for table_name, table in tables.items():
            # If table name is provided, only generate DDL for those tables. Otherwise, generate DDL for all tables.
            if len(table_names) > 0 and table_name not in table_names:
                continue
            # Start the DDL statement
            table_comment = f'-- {table.comment}' if table.comment else ''
            ddl = f"CREATE TABLE {table_name} {table_comment} \n (\n"
            for column in table.columns:
                column: Column
                # get column description
                column_comment = f'-- {column.comment}' if column.comment else ''
                ddl += f"  {column.name} {column.type.__visit_name__} {column_comment},\n"
            ddl = ddl.rstrip(',\n') + "\n)"  # Remove the last comma and close the CREATE TABLE statement
            table_info[table_name] = {}
            table_info[table_name]['ddl'] = ddl
            table_info[table_name]['description'] = table.comment

            logger.info(f'added table {table_name} to table_info dict')

        return table_info

    @classmethod
    def get_db_url_by_connection(cls, connection: ConnectConfigEntity):
        db_url = cls.get_db_url(connection.db_type, connection.db_user, connection.db_pwd, connection.db_host,
                                connection.db_port, connection.db_name)
        return db_url
