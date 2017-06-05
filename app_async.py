import asyncio
from aiohttp import ClientSession
import aiofiles
import json
import csv

headers = {
	'Content-Type': 'application/json; charset=UTF-8',
	'Referer': 'https://geomap.ffiec.gov/FFIECGeocMap/GeocodeMap1.aspx',
	'Accept': 'application/json, text/javascript, */*; q=0.01',
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5',
	'Origin': 'https://geomap.ffiec.gov',
	'X-Requested-With': 'XMLHttpRequest',
}

def normalize_responses(result):
	json_parsed = json.loads(result)

	validated_data = {}
	validated_data['address'] = json_parsed.get('d').get('sMatchAddr')
	validated_data['state'] = json_parsed.get('d').get('sStateCode')
	validated_data['country'] = json_parsed.get('d').get('sCountyCode')
	validated_data['msa'] = json_parsed.get('d').get('sMSACode')
	validated_data['tract'] = json_parsed.get('d').get('sTractCode')

	json_validated_data = json.dumps(validated_data)
	return json_validated_data

async def main():
	with open('TestData100.csv') as csvfile:
		has_header = csv.Sniffer().has_header(csvfile.read(1024))
		csvfile.seek(0)
		incsv = csv.reader(csvfile, delimiter=',')
		if has_header:
			next(incsv)
		async with ClientSession() as session:
			tasks = []
			for row in incsv:
				address = row[2] + ',' + row[5]
				task = asyncio.ensure_future(getData(address, session))
				tasks.append(task)

			responses = await asyncio.gather(*tasks)

async def getData(address, session):
	url = "https://geomap.ffiec.gov/FFIECGeocMap/GeocodeMap1.aspx/GetGeocodeData"
	data = '{sSingleLine: "' + address + '", iCensusYear: "2017"}'
	async with session.post(url, headers=headers, data=data) as response:
		data = await response.text()
		result = normalize_responses(data)
	async with aiofiles.open('data.json', 'a') as file:
		await file.write(result)

# Program starts here...
if __name__ == '__main__':

	loop = asyncio.get_event_loop()
	future = asyncio.ensure_future(main())
	loop.run_until_complete(future)
