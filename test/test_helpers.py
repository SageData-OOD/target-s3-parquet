import pytest
import logging

from target_s3_parquet.helpers import flatten, flatten_schema


def test_flatten():
    in_dict = {
        "key_1": 1,
        "key_2": {"key_3": 2, "key_4": {"key_5": 3, "key_6": ["10", "11"]}},
    }
    expected = {
        "key_1": 1,
        "key_2__key_3": 2,
        "key_2__key_4__key_5": 3,
        "key_2__key_4__key_6": "['10', '11']",
    }

    output = flatten(in_dict)
    assert output == expected

def test_flatten_schema():
    in_dict = {
        'key_1': {'type': ['null', 'integer']},
        'key_2': {
            'type': ['null', 'object'],
            'properties': {
                'key_3': {'type': ['null', 'string']},
                'key_4': {
                    'type': ['null', 'object'],
                    'properties': {
                        'key_5' : {'type': ['null', 'integer']},
                        'key_6' : {
                            'type': ['null', 'array'],
                            'items': {
                                'type': ['null', 'object'],
                                'properties': {
                                    'key_7': {'type': ['null', 'number']},
                                    'key_8': {'type': ['null', 'string']}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    expected = [
             'key_1',
             'key_2__key_3',
             'key_2__key_4__key_5',
             'key_2__key_4__key_6'
    ]

    output = flatten_schema(in_dict)
    assert output == expected

def test_flatten_schema_2(caplog):
    in_dict = {
        "id": {
            "type": "integer"
        },
        "created_at": {
            "type": "string",
            "format": "date-time"
        },
        "updated_at": {
            "type": "string",
            "format": "date-time"
        },
        "email": {
            "type": "string"
        },
        "last_surveyed": {
            "anyOf": [
                {
                    "type": "null"
                },
                {
                    "type": "string",
                    "format": "date-time"
                }
            ]
        },
        "external_created_at": {
            "type": [
                "integer",
                "null"
            ]
        },
        "page_views_count": {
            "type": "integer"
        }
    }

    expected = [
        'id',
        'created_at',
        'updated_at',
        'email',
        'last_surveyed',
        'external_created_at',
        'page_views_count'
    ]

    with caplog.at_level(logging.WARNING):
        output = flatten_schema(in_dict)
        for record in caplog.records:
            assert "SCHEMA with limitted support on field last_surveyed" \
                in record.message
    assert output == expected

def test_flatten_schema_empty():
    in_dict = dict()
    assert list() == flatten_schema(in_dict)