#!/usr/bin/env python3

import singer
from datetime import datetime

logger = singer.get_logger()


def get_target_key(stream, prefix=None, timestamp=None, compression="", naming_convention=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        naming_convention = '{stream}-{timestamp}{compression}.parquet' # o['stream'] + '-' + now + '.csv'
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    key = naming_convention
    
    # replace simple tokens
    for k, v in {
        '{stream}': stream,
        '{timestamp}': timestamp,
        '{compression}': compression,
        '{date}': datetime.now().strftime('%Y-%m-%d')
    }.items():
        if k in key:
            key = key.replace(k, v)

    # replace dynamic tokens
    # todo: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split('/')[-1]
        key = key.replace(filename, f'{prefix}{filename}')
    return key
