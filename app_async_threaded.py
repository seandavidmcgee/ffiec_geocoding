import aiohttp
import asyncio
import json
import csv
import os

concurrent = 200
url_routes = []
headers = {
	'Content-Type': 'application/json; charset=UTF-8',
	'Referer': 'https://geomap.ffiec.gov/FFIECGeocMap/GeocodeMap1.aspx',
	'Accept': 'application/json, text/javascript, */*; q=0.01',
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5',
	'Origin': 'https://geomap.ffiec.gov',
	'X-Requested-With': 'XMLHttpRequest',
}
dest_url = "https://geomap.ffiec.gov/FFIECGeocMap/GeocodeMap1.aspx/GetGeocodeData"

def handle_req(data):
	json_geocode = json.loads(data[0].decode('utf-8'))['d']
	address_id = data[1]
	address = json_geocode['sMatchAddr']
	state = json_geocode['sStateCode']
	country = json_geocode['sCountyCode']
	msa = json_geocode['sMSACode']
	tract = json_geocode['sTractCode']
	return [address_id, address, state, country, msa, tract]

def chunked_http_client(num_chunks, s):
	# Use semaphore to limit number of requests
	semaphore = asyncio.Semaphore(num_chunks)
	@asyncio.coroutine
	# Return co-routine that will work asynchronously and respect
	# locking of semaphore
	def http_get(url):
		nonlocal semaphore
		with (yield from semaphore):
			address_data = url.split(',')
			address = address_data[1] + ',' + address_data[2]
			address_id = address_data[0]
			data = '{sSingleLine: "' + address + '", iCensusYear: "2017"}'
			response = yield from s.post(dest_url, headers=headers, data=data)
			body = yield from response.content.read()
			yield from response.wait_for_close()
		return body, address_id
	return http_get

def run_experiment(urls, _session):
	http_client = chunked_http_client(num_chunks=concurrent, s=_session)
	# http_client returns futures, save all the futures to a list
	tasks = [http_client(url) for url in urls]
	response = []
	# wait for futures to be ready then iterate over them
	for future in asyncio.as_completed(tasks):
		data = yield from future
		try:
			out = handle_req(data)
			output_size = os.path.getsize('data.csv')
			with open('data.csv','a',newline='') as f:
				w = csv.writer(f)
				w.writerow(out)
		except Exception as err:
			print("Error for {0} - {1}".format(err))
			out = [s, 999, 0, 0]
		response.append(out)
	return response

# Run:
with aiohttp.ClientSession() as session:  # We create a persistent connection
	with open('branchgeo.csv') as csvfile:
		has_header = csv.Sniffer().has_header(csvfile.read(1024))
		csvfile.seek(0)
		incsv = csv.reader(csvfile, delimiter=',')
		if has_header:
			next(incsv)
		with open('data.csv','w',newline='') as f:
			w = csv.writer(f)
			w.writerow(["Account Number", "Matched Address", "State Code", "County Code", "MSA", "Tract"])
			for row in incsv:
				address = row[0] + ',' + row[2] + ',' + row[5]
				url_routes.append(address)
		loop = asyncio.get_event_loop()
		calc_routes = loop.run_until_complete(run_experiment(url_routes, session))
