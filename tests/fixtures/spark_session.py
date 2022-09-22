import pytest
from pyspark import SparkConf, SparkContext, sql


@pytest.fixture(scope="session")
def spark_session(request):
    """
    Fixture for creating a spark session
    :param request: pytest.FixtureRequest object
    """
    conf = (
        SparkConf().setMaster("local").setAppName(
            "pytest-pyspark-local-testing").set("spark.driver.host", "localhost")
    )
    sc = SparkContext(conf=conf)

    session = sql.SparkSession(sc)
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.conf.set("spark.sql.caseSensitive", "false")
    request.addfinalizer(lambda: sc.stop())

    return session
