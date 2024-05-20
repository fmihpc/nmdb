#! /usr/bin/env python3
'''
Program for listing realtime neutron monitor data from postgresql.

Copyright 2024 Finnish Meteorological Institute

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
from datetime import datetime, timedelta, timezone
import os

try:
	import psycopg2
except Exception as e:
	print("Couldn't import psycopg2, try pip3 install --user psycopg2:", e)
	exit(1)

parser = argparse.ArgumentParser(
	description = 'Plots neutron monitor data from postgresql.',
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
	'--start',
	default = (datetime.now(timezone.utc) - timedelta(days = 1)).isoformat(timespec = 'seconds').replace('+00:00', 'Z'),
	help = 'Start plot from this time (UTC, default 1 day ago).')
parser.add_argument(
	'--end',
	default = datetime.now(timezone.utc).isoformat(timespec = 'seconds').replace('+00:00', 'Z'),
	help = 'End plot at this time (UTC, default now).')

args = parser.parse_args()

try:
	_ = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S%z')
except Exception as e:
	print('Invalid plot start time:', e)
	exit(1)
try:
	_ = datetime.strptime(args.end, '%Y-%m-%dT%H:%M:%S%z')
except Exception as e:
	print('Invalid plot end time:', e)
	exit(1)

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

cursor.execute('select * from ' + args.table + ' where datetime >= %s and datetime <= %s', (args.start, args.end))
result = cursor.fetchall()
cursor.close()
connection.close()

data = dict()
dts = set()
for items in result:
	dt, station, value = items
	dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
	dts.add(dt)
	if not station in data:
		data[station] = dict()
	data[station][dt] = value

print('# time (UTC), value at station ', end = '')
stations = sorted(data.keys())
for i in range(len(stations)):
	print(stations[i], end = ', ')
dts = sorted(dts)
print('\n# missing observation(s) given as nan')
for dt in dts:
	print(dt.isoformat(timespec = 'seconds'), end = ' ')
	for i in range(len(stations)):
		station = stations[i]
		value = float('nan')
		if dt in data[station]:
			value = data[station][dt]
		print(value, end = ' ')
	print()
print()
