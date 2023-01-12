import pytest
import io
from datetime import datetime
from decimal import Decimal
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from pandas.testing import assert_frame_equal
import json
import logging

# from os import walk
import glob
import os
import target_s3_parquet
from target_s3_parquet import persist_messages

target_s3_parquet.upload_files_to_s3 = dict

#### TEMP DEBUG



@pytest.fixture
def expected_df_1():
    return pa.table({
        'str': ['value1', 'value2', 'value3'],
        'int': [1, None, 3],
        'decimal': [Decimal('0.1'), Decimal('0.2'), Decimal('0.3')],
        'date': ['2021-06-11', '2021-06-12', '2021-06-13'],
        'datetime': ['2021-06-11T00:00:00.000000Z', '2021-06-12T00:00:00.000000Z', '2021-06-13T00:00:00.000000Z'],
        'boolean': [True, True, False]
    }).to_pandas()

@pytest.fixture
def input_messages_1():
    return """\
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

@pytest.fixture
def input_messages_1_reorder():
    return """\
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

def test_persist_messages(input_messages_1, expected_df_1):
    # content of test_persist.expected.pkl based on : [{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"},{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"}]

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_1.encode()), encoding="utf-8"
    )

    persist_messages(input_messages, f"test_{timestamp}", None)

    filename = [f for f in glob.glob(f"test_{timestamp}/*.parquet")]

    df = ParquetFile(filename[0]).read().to_pandas()

    for f in filename:
        os.remove(f)
    os.rmdir(f"test_{timestamp}")

    assert_frame_equal(df, expected_df_1)

def test_persist_messages_invalid_sort(input_messages_1_reorder):
    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_1_reorder.encode()), encoding="utf-8"
    )

    with pytest.raises(ValueError,
                       match="A record for stream test was encountered before a corresponding schema"):
        persist_messages(input_messages, "test_", None)
