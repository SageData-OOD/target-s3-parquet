#!/usr/bin/env python3
import gzip
import shutil

import singer
from datetime import datetime

import snappy

logger = singer.get_logger()
default_naming_convention = '{stream}-{timestamp}'


def do_outer_compression(filename, compression_method):
    if compression_method == "gzip":
        compressed_file = f"{filename}.gz"
        with open(filename, 'rb') as f_in:
            with gzip.open(compressed_file, 'wb') as f_out:
                logger.info(f"Compressing file as '{compressed_file}'")
                shutil.copyfileobj(f_in, f_out)
    elif compression_method == "snappy":
        compressed_file = f"{filename}.snappy"
        with open(filename, 'rb') as f_in:
            with open(compressed_file, 'wb') as f_out:
                snappy.stream_compress(f_in, f_out)
    else:
        raise NotImplementedError(
            "Compression type '{}' is not supported for outer compression. "
            "Expected: 'none' or 'gzip' or 'snappy'"
                .format(compression_method)
        )
    return compressed_file


def get_extension_mapping(compression_method):
    if not compression_method:
        return None, ""

    extension_mapping = {
        "SNAPPY": ".snappy",
        "GZIP": ".gz",
        # "BROTLI": ".br",
        # "ZSTD": ".zstd",
        # "LZ4": ".lz4",
    }
    compression_extension = extension_mapping.get(compression_method.upper())
    if compression_extension is None:
        logger.warning("unsupported compression method.")
        compression_extension = ""
        compression_method = None
    return compression_method, compression_extension


def get_target_key(stream, prefix=None, timestamp=None, compression_type=None, compression_extension="",
                   naming_convention=None):
    """Creates and returns an S3 key for the message"""

    if not naming_convention:
        naming_convention = default_naming_convention

    if naming_convention[-8:] == ".parquet":
        naming_convention = naming_convention[:-8]

    naming_convention += ('{extension}.parquet' if compression_type == "inner"
                          else '.parquet{extension}')

    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    key = naming_convention

    # replace simple tokens
    for k, v in {
        '{stream}': stream,
        '{timestamp}': timestamp,
        '{extension}': compression_extension,
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
