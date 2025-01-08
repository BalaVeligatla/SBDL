from datetime import date, datetime
from chispa import assert_df_equality
import pytest
from pyspark.sql.types import Row, StructType, StructField, StringType, NullType, TimestampType, ArrayType

from lib.ConfigLoader import get_config
from lib.DataLoader import read_accounts, read_parties
from lib.Transformations import get_contract
from lib.Utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope="session")
def expected_party_rows():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32))]


@pytest.fixture(scope="session")
def expected_contract_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")


def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"


def test_read_accounts(spark):
    accounts_df = read_accounts(spark, "LOCAL", False, None)
    assert accounts_df.count() == 8


# def test_read_parties(spark, expected_party_rows):
#     actual_party_rows = read_parties(spark, "LOCAL", False, None).collect()
#     assert actual_party_rows == expected_party_rows

def test_get_contract(spark):
    accounts_df = read_accounts(spark, "LOCAL", False, None)
    actual_contract_df = get_contract(accounts_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    assert_df_equality(expected_contract_df, actual_contract_df, ignore_schema=True)
