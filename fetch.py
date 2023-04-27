#! /usr/bin/env python3
'''
Program for adding realtime neutron monitor data to postgresql.

Copyright 2023 Finnish Meteorological Institute

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


Aurthor(s): Ilja Honkonen
'''

import argparse
from datetime import datetime
import os
from time import sleep

try:
	import psycopg2
except:
	print("Couldn't import psycopg2, try pip3 install --user psycopg2")
	exit(1)
try:
	import requests
except:
	print("Couldn't import requests, try pip3 install --user requests")
	exit(1)

parser = argparse.ArgumentParser(
	description = 'Fetches neutron monitor data into postgresql.',
	formatter_class = argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
	'--db-name',
	default = 'test',
	metavar = 'N',
	help = 'Operate on database named N.')
parser.add_argument(
	'--db-user',
	default = 'test',
	metavar = 'U',
	help = 'Operate on database as user U.')
parser.add_argument(
	'--db-password-env',
	default = 'NMDBPW',
	metavar = 'S',
	help = 'Use password from env var S for database connection.')
parser.add_argument(
	'--db-host',
	default = 'localhost',
	metavar = 'H',
	help = 'Operate on database at address H.')
parser.add_argument(
	'--db-port',
	type = int,
	default = 5432,
	metavar = 'P',
	help = 'Operate on database at port P.')
parser.add_argument(
	'--table',
	default = 'test',
	metavar = 'T',
	help = 'Use table T in database N.')
parser.add_argument(
	'--url',
	default = 'https://www.nmdb.eu/rt/realtime.txt',
	help = 'URL for downloading new data.')

args = parser.parse_args()


if not args.db_password_env in os.environ:
	print('Environment variable for db password', args.db_password_env, "doesn't exist")
	exit(1)

try:
	connection = psycopg2.connect(
		dbname = args.db_name,
		user = args.db_user,
		password = os.environ[args.db_password_env],
		host = args.db_host,
		port = args.db_port)
except Exception as e:
	print("Couldn't connect to database: ", e)
	exit(1)

cursor = connection.cursor()

# possibly create db
cursor.execute('create table if not exists ' + args.table + ' (datetime varchar, station varchar, value real)')
connection.commit()

# TODO: make sure that correct columns exist

text = requests.get(args.url).text

data = dict()
for line in text.splitlines():
	line = line.strip()
	if line.startswith('#'):
		continue
	try:
		items = line.split(';')
		dt = datetime.strptime(items[0], '%Y-%m-%d %H:%M:%S').isoformat(timespec = 'seconds')
		station = items[1]
		value = float(items[2])
		if not station in data:
			data[station] = dict()
		data[station][dt] = value
	except:
		print("Couldn't process line:", line)

# exclude old data
new_data = dict()
for station in data.keys():
	cursor.execute('select max(datetime) from ' + args.table + ' where station = %s', [station])
	result = cursor.fetchone()[0]
	if result == None:
		result = '0000-00-00T00:00:00'
	for dt in data[station].keys():
		if dt <= result:
			continue
		if not station in new_data:
			new_data[station] = dict()
		new_data[station][dt] = data[station][dt]

try:
	cursor.execute('begin transaction')
	cursor.execute('lock table ' + args.table + ' in exclusive mode nowait')
except Exception as e:
	print('Someone already writing to table', args.table, 'of database', args.db_name)
	exit()

inserted = 0
for station in new_data.keys():
	for dt in new_data[station].keys():
		cursor.execute('select exists (select from ' + args.table + ' where datetime = %s and station = %s)', (dt, station))
		result = cursor.fetchone()[0]
		if not result:
			cursor.execute('insert into ' + args.table + ' (datetime, station, value) values (%s, %s, %s)', (dt, station, new_data[station][dt]))
			inserted += 1
connection.commit()
cursor.close()
connection.close()
print(inserted, 'new values added to database')
