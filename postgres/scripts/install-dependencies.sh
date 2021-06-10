#!/bin/bash

set -e
set -o pipefail

pip3 install -U psycopg2-binary duckdb==0.2.6 python-dateutil