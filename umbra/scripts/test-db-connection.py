#!/usr/bin/env python3

import os
import psycopg2

pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)
pg_con.close()
