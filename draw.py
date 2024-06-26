#! /usr/bin/env python3
'''
Program for plotting realtime neutron monitor data from postgresql.

Copyright 2023, 2024 Finnish Meteorological Institute

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
	import matplotlib.dates as mdates
	from matplotlib.pyplot import gca, plot, savefig, title
except Exception as e:
	print("Couldn't import from matplotlib, try pip3 install --user matplotlib:", e)
	exit(1)
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
parser.add_argument(
	'--format',
	default = '%H:%M',
	help = 'Format of values on time axis.')
parser.add_argument(
	'--title',
	default = 'Realtime nmdb.eu data',
	help = 'Plot title to use.')
parser.add_argument(
	'--path',
	default = 'draw.png',
	help = 'Save plot in PATH.')

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
for items in result:
	dt, station, value = items
	dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
	if not station in data:
		data[station] = []
	data[station].append((dt, value))

title(args.title)
for station in sorted(data.keys()):
	data[station].sort()
	dts, values = [], []
	for i in data[station]:
		dts.append(i[0])
		values.append(i[1])
	plot(dts, values, '*', markersize = 1)
gca().xaxis.set_major_formatter(mdates.DateFormatter(args.format))
savefig(args.path, dpi = 300)
