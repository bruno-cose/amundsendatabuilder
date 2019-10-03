import logging

from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from itertools import groupby


TableKey = namedtuple('TableKey', ['schema_name', 'table_name'])

LOGGER = logging.getLogger(__name__)


class OracleMetadataExtractor(Extractor):
    """
    Extracts Oracle Autonomous Databas table and column metadata from underlying meta store database using SQLAlchemyExtractor
    """
    # SELECT statement from oracle user_tab_columns to extract table and column metadata
    # Column and Table Descriptions is taken from user_tab_columns and user_col_comments
    SQL_STATEMENT = """
    SELECT {cluster_source} as "cluster",(SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual) as "schema_name", a.table_name as "name", c.comments as "description",
    a.column_name as "col_name", a.data_type as "col_type", d.comments as "col_description", a.column_id as "col_sort_order"
    FROM user_tab_columns a
    JOIN user_all_tables b ON a.table_name = b.table_name
    JOIN user_tab_comments c  ON a.table_name = c.table_name
    JOIN user_col_comments d ON a.table_name = d.table_name AND a.column_name =  d.column_name
    ORDER BY b.cluster_name,(SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual),a.table_name,a.column_id
    """

    # CONFIG KEYS
    CLUSTER_KEY = 'cluster_key'
    USE_CATALOG_AS_CLUSTER_NAME = 'use_catalog_as_cluster_name'
    DATABASE_KEY = 'database_key'


    #Default values
    DEFAULT_CLUSTER_NAME = 'master'
    DEFAULT_DATABASE_NAME = 'autonomous'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {CLUSTER_KEY: DEFAULT_CLUSTER_NAME, USE_CATALOG_AS_CLUSTER_NAME: False,
         DATABASE_KEY: DEFAULT_DATABASE_NAME }
    )

    def init(self, conf):
        # type: (ConfigTree) -> None
        conf = conf.with_fallback(OracleMetadataExtractor.DEFAULT_CONFIG)
        self._cluster = '{}'.format(conf.get_string(OracleMetadataExtractor.CLUSTER_KEY))

        ##setting cluster name based on config
        if conf.get_bool(OracleMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            cluster_source = "b.cluster_name"
        else:
            cluster_source = "'{}'".format(self._cluster)

        database = conf.get_string(OracleMetadataExtractor.DATABASE_KEY, default='oracle')

        self._database = database

        self.sql_stmt = OracleMetadataExtractor.SQL_STATEMENT.format(cluster_source=cluster_source)
        LOGGER.info('SQL for oracle metadata: {}'.format(self.sql_stmt))

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope())\
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[TableMetadata, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.oracle_metadata'

    def _get_extract_iter(self):
        # type: () -> Iterator[TableMetadata]
        """
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        """
        for key, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(ColumnMetadata(row['col_name'], row['col_description'],
                                              row['col_type'], row['col_sort_order']))

            yield TableMetadata(self._database, last_row['cluster'],
                                last_row['schema_name'],
                                last_row['name'],
                                last_row['description'],
                                columns)

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Dict[str, Any]]
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield row
            row = self._alchemy_extractor.extract()

    def _get_table_key(self, row):
        # type: (Dict[str, Any]) -> Union[TableKey, None]
        """
        Table key consists of schema and table name
        :param row:
        :return:
        """
        if row:
            return TableKey(schema_name=row['schema_name'], table_name=row['name'])

        return None
