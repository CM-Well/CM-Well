# Copyright 2018 Thomson Reuters

# Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.

# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and
# limitations under the License.

# Example code for using the CM-Well consume/bulk-consume APIs to extract data
# Tested in Python 3.x
# Requires the python requests library. Install with pip install requests
import requests
import sys
from urllib.parse import urlparse

# Print to std err
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    
# Given a CM-Well Infoton base (e.g /permid.org) create a consumer token
def create_consumer(url_base):
	token = None
	url = url_base+'?op=create-consumer'
	try:
		response = requests.get(url)
	except Exception as e:
		eprint ('Error in connect ' , e)
		return
	if response.status_code == requests.codes.ok:
		token = response.headers['X-CM-WELL-POSITION']
	else:
		raise Exception(u"Invalid response from server {0}".format(response))
	return token
 
# Consume a batch of Infotons from CM-Well. Params:
# url_base - as passed to create_consumer
# format - any supported by the API, for example ntriples
# token - the token returned from create_consumer or the last invocation of this function
# bulk_consume - if True, use the bulk consumer, else use the consume API
# Prints Infotons to std out, returns next token
def consume_nextbatch(url_base,format,token,bulk_consume):
	if bulk_consume:
		url = url_base+'/_bulk-consume?format='+format+'&position='+token
	else:
		url = url_base+'/_consume?format='+format+'&position='+token
	ret_token = None
	payload = None
	try:
		response = requests.get(url, stream=True)
		if response.status_code == requests.codes.ok:
			ret_token = response.headers['X-CM-WELL-POSITION']
			for line in response.iter_lines(chunk_size=1024,decode_unicode=True): 
				if line:
					print(line)
		elif response.status_code == requests.codes.no_content:
			ret_token = None
		else:
			raise Exception(u"Invalid response from server {0}".format(response))
	except httplib.IncompleteRead as e:
		eprint('0 bytes from the server')
	except Exception as e:
		eprint ('Error in connect ' , e)
	finally:
		response.close()
	return ret_token

if len(sys.argv) < 4:
	eprint ('Bulk export data from a CM-Well instance using the consume methods')
	eprint ('Can be restarted using the current token - if not provided starts from beginning')
	eprint ('Errors and current token printed to the error stream')
	eprint ('Data printed to the output stream for onward redirection')
	eprint ('Format is per documentation (e.g. ntriples etc)')
	eprint ('Method is either consume or bulk-consume')
	eprint ()
	eprint ('Usage: python bulk_consume.py <url> <format> <method> <token>  ')
	sys.exit(1)

url = sys.argv[1]
o = urlparse(url)
url_base = o.scheme+'://'+o.netloc
format = sys.argv[2]
bulk_consume = len(sys.argv) > 3 and sys.argv[3] == 'bulk-consume'

if len(sys.argv) > 4:
	token = sys.argv[4]
else:
	token = create_consumer(url)
count = 0
eprint('Count\tToken')
while token != None:
	token = consume_nextbatch(url_base,format,token,bulk_consume)
	count = count+1
	eprint("{0}\t{1}".format(count,token))
