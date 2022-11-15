#!/usr/bin/env python3
import argparse
from datetime import datetime
from io import TextIOWrapper
import simplejson as json
from jsonschema.validators import Draft4Validator
import os
import pyarrow as pa
from pyarrow.parquet import ParquetWriter
import singer
import sys
import psutil
import time
import threading
# import gc
from enum import Enum

from .helpers import flatten, flatten_schema
from target_s3_parquet import s3
from target_s3_parquet import utils

_all__ = ["main"]

LOGGER = singer.get_logger()
# LOGGER.setLevel(logging.DEBUG)

def create_dataframe(list_dict):
    fields = set()
    for d in list_dict:
        fields = fields.union(d.keys())
    dataframe = pa.table({f: [row.get(f) for row in list_dict] for f in sorted(fields)})
    return dataframe

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


class MemoryReporter(threading.Thread):
    """Logs memory usage every 30 seconds"""

    def __init__(self):
        self.process = psutil.Process()
        super().__init__(name="memory_reporter", daemon=True)

    def run(self):
        while True:
            LOGGER.info(
                "Virtual memory usage: %.2f%% of total: %s",
                self.process.memory_percent(),
                self.process.memory_info(),
            )
            time.sleep(30.0)


def emit_records_counter_metrics(record_counter, stream_name):
    metric = {"type": "counter", "metric": "record_count", "value": record_counter.get(stream_name),
              "tags": {"count_type": "table_rows_persisted", "table": stream_name}}
    LOGGER.info('\nINFO METRIC: %s', json.dumps(metric))
    sys.stderr.flush()


def persist_messages(
        messages,
        config,
        s3_client,
        file_size=-1
):
    destination_path = "/tmp/"
    compression_method = config.get("compression_method")
    compression_type = config.get("compression_type", "inner")
    streams_in_separate_folder = False

    # Static information shared among processes
    schemas = {}
    validators = {}
    files_to_upload = set()
    record_counter = dict()

    # TODO: add metadata columns?
    key_properties = {}
    now = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")

    # The target is prepared to accept all the compression methods provided by the Pandas module,
    # with the mapping below,
    compression_method, compression_extension = utils.get_extension_mapping(compression_method)

    inner_extension = compression_extension if compression_type == "inner" else ""  # file.gz.parquet
    outer_extension = compression_extension if compression_type == "outer" else ""  # file.parquet.gz

    filename_separator = "-"
    if streams_in_separate_folder:
        LOGGER.info("writing streams in separate folders")
        filename_separator = os.path.sep
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    # End of Static information shared among processes

    def process_messages(message_buffer: TextIOWrapper):
        state = None
        current_stream_name = None
        records = {}

        for message in message_buffer:
            LOGGER.debug(f"target-parquet got message: {message}")
            try:
                message = singer.parse_message(message).asdict()
            except json.decoder.JSONDecodeError:
                raise Exception("Unable to parse:\n{}".format(message))

            message_type = message["type"]
            if message_type == "RECORD":
                if message["stream"] not in schemas:
                    raise ValueError(
                        "A record for stream {} was encountered before a corresponding schema".format(
                            message["stream"]
                        )
                    )
                stream_name = message["stream"]
                record = message.get("record")
                validators[message["stream"]].validate(record)
                flattened_record = flatten(record)
                # Once the record is flattened, it is added to the final record list, which will be stored in
                # the parquet file.
                if (stream_name != current_stream_name) and (current_stream_name != None):
                    write_file(
                        current_stream_name,
                        records.pop(current_stream_name)
                    )
                    upload_files_to_s3()
                current_stream_name = stream_name
                if type(records.get(stream_name)) != list:
                    records[stream_name] = [flattened_record]
                else:
                    records[stream_name].append(flattened_record)
                    if (file_size > 0) and \
                            (not len(records[stream_name]) % file_size):
                        write_file(
                            current_stream_name,
                            records.pop(current_stream_name)
                        )
                        upload_files_to_s3()
                state = None
            elif message_type == "STATE":
                LOGGER.debug("Setting state to {}".format(message["value"]))
                state = message["value"]
            elif message_type == "SCHEMA":
                stream = message["stream"]
                validators[stream] = Draft4Validator(message["schema"])

                properties = message["schema"]["properties"]
                schemas[stream] = flatten_schema(properties)
                LOGGER.info(f"Schema: {schemas[stream]}")
                # key_properties[stream] = message["key_properties"]
            else:
                LOGGER.debug(
                    "Unknown message type {} in message {}".format(
                        message["type"], message
                    )
                )

        if current_stream_name and current_stream_name in records:
            write_file(
                current_stream_name,
                records.pop(current_stream_name)
            )

        LOGGER.info("Upload files to S3...")
        
        upload_files_to_s3()
        # Emit record_count metrics
        for stream_name in record_counter:
            emit_records_counter_metrics(record_counter, stream_name)

        return state

    def write_file(current_stream_name, record):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")
        LOGGER.info(f"Writing files from {current_stream_name} stream")
        dataframe = create_dataframe(record)
        if streams_in_separate_folder and not os.path.exists(
                os.path.join(destination_path, current_stream_name)
        ):
            os.makedirs(os.path.join(destination_path, current_stream_name))
        filename = (
                current_stream_name
                + filename_separator
                + timestamp
                + inner_extension
                + ".parquet"
        )
        filepath = os.path.expanduser(os.path.join(destination_path, filename))

        LOGGER.info("Filepath: %s, Records: %d", filepath, len(record))

        # Create target s3 key
        target_key = utils.get_target_key(current_stream_name,
                                          prefix=config.get('s3_key_prefix', ''),
                                          timestamp=timestamp,
                                          compression_type=compression_type,
                                          compression_extension=compression_extension,
                                          naming_convention=config.get('naming_convention'))

        #keep current_stream_name
        files_to_upload.add((filepath, target_key, current_stream_name))

        # if not file_writer.get(current_stream_name):
        #     file_writer[current_stream_name] = ParquetWriter(filepath,
        #                                                      dataframe.schema,
        #                                                      compression=compression_method
        #                                                      if compression_type == "inner" else None
        #                                                      )
        # file_writer[current_stream_name].write_batch(dataframe)

        with ParquetWriter(filepath,
                    dataframe.schema,
                    compression=compression_method
                    if compression_type == "inner" else None
                    ) as fw:
            fw.write_table(dataframe)

        if current_stream_name not in record_counter:
            record_counter[current_stream_name] = 0
        record_counter[current_stream_name] += len(record)

        # explicit memory management. This can be useful when working on very large data groups
        del dataframe
        return filepath

    def upload_files_to_s3():
        LOGGER.info(f"Writing {len(files_to_upload)} files")
        compressed_file = None
        for filename, target_key, stream_name in files_to_upload:
            if compression_method and outer_extension:
                compressed_file = utils.do_outer_compression(filename, compression_method)

            s3.upload_file(compressed_file or filename,
                           s3_client,
                           config.get('s3_bucket'),
                           target_key,
                           encryption_type=config.get('encryption_type'),
                           encryption_key=config.get('encryption_key'))

            # Remove the local file(s)
            os.remove(filename)

        files_to_upload.clear()

    return process_messages(messages)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")

    args = parser.parse_args()
    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}
        level = config.get("logging_level", None)
        if level:
            LOGGER.setLevel(level)
    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    # if LOGGER.level == 0:
    #     MemoryReporter().start()

    s3_client = s3.create_client(config)
    state = persist_messages(
        input_messages,
        config,
        s3_client,
        # batch every 20k records
        int(config.get("file_size", 20 * 1000))
    )

    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
