#!/usr/bin/env python

from setuptools import setup

setup(
    name="target-s3-parquet",
    version="0.2.2",
    description="Singer.io target for writing into parquet files",
    author="Rafael 'Auyer' Passos",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_parquet"],
    install_requires=[
        "jsonschema==2.6.0",
        "singer-python==5.12.1",
        "pyarrow==10.0.0",
        "psutil==5.8",
        "boto3==1.23.7",
        "python-snappy==0.6.1"
    ],
    extras_require={
        'dev': [
            'pytest==6.2.4',
            'pandas==1.2.4']
    },
    entry_points="""
          [console_scripts]
          target-s3-parquet=target_s3_parquet:main
      """,
    packages=["target_s3_parquet"],
)
