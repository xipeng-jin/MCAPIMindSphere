from ConstantUpload import ConstantUpload
from pathlib import Path
import requests
from pprint import pprint
from time import sleep

data_point_id_lookup_table = {
    'reportedHastrate': '1548700073499',
    'currentBalance': '1548700196440',
    'hashrateEth': '1548700137803'
}
onboarding_json_file_location = Path('SouthBoundTokens/Initial.json')
registration_json_file_location = Path('SouthBoundTokens/cryptoRegistration.json')
authorization_json_file_location = Path('SouthBoundTokens/cryptoAuthorization.json')
my_agent = ConstantUpload(onboarding_json_file_location,
                          registration_json_file_location,
                          authorization_json_file_location)

url = "https://eth.nanopool.org/api/v1/load_account/0x0ac7e3f5060700cf30da11a9f1a503dd8c471840"
headers = {}


while True:
    response = requests.request("GET", url, headers=headers)
    body = response.json()
    my_agent.data_to_dict = {
        'reportedHastrate': body['data']['userParams']['reported'],
        'currentBalance': body['data']['userParams']['balance'],
        'hashrateEth': body['data']['userParams']['hashrate']
    }
    my_agent.create_an_entry_to_iot_timeseries(data_point_id_lookup_table)
    mulipart_boundary = my_agent.write_multipart('1548700219026', 'my_multipart.txt')
    pprint(my_agent.iot_timeseries)
    my_agent.mindsphere_exchange_api(mulipart_boundary, my_agent.multipart_message)
    my_agent.check_every_token()
    my_agent.iot_timeseries = []
    # sleep(60)