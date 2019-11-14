from ConstantUpload import ConstantUpload
from pathlib import Path
import requests
from pprint import pprint
from time import sleep
import pandas as pd
import json
from os import walk, path

for (dirpath, dirnames, filenames) in walk(path.relpath('BulkUploadReferenceData/PumpData')):
    filter_files_for_csv = filter(lambda x: '.csv' in x, filenames)
    df_list = [pd.read_csv(Path(dirpath, file)) for file in filter_files_for_csv]

df = pd.concat(df_list, sort=True)


data_source = '1549487324862'
n=1

data_point_id_lookup_table = {
    'PressureOut': '1549487273267',
    'PressueIn': '1549487252514',
    'StuffingBoxTemp': '1549487231362',
    'Passage': '1549487192714',
    'MotorCurrent': '1549487140173'
}

onboarding_json_file_location = path.relpath('SouthBoundTokens/InitAll.json')
registration_json_file_location = path.relpath('SouthBoundTokens/RegAll.json')
authorization_json_file_location = path.relpath('SouthBoundTokens/AccAll.json')
my_agent = ConstantUpload(onboarding_json_file_location,
                          registration_json_file_location,
                          authorization_json_file_location)

print('Starting Upload')

pump_index = 0
place_holder = pump_index
while True:
    try:
        for index, row in df[pump_index:].iterrows():
            my_agent.data_to_dict = {
                'PressureOut': row['PressureOut'],
                'PressueIn': row['PressureIn'],
                'StuffingBoxTemp': row['StuffingBoxTemp'],
                'Passage': row['Passage'],
                'MotorCurrent': row['MotorCurrent']
            }
            my_agent.create_an_entry_to_iot_timeseries(data_point_id_lookup_table)
            mulipart_boundary = my_agent.write_multipart(data_source)
            pprint(my_agent.iot_timeseries)
            my_agent.mindsphere_exchange_api(mulipart_boundary, my_agent.multipart_message)
            my_agent.check_every_token()
            my_agent.iot_timeseries = []
            place_holder = index
            print(index)
            sleep(n)
        print('End of File. Restarting Upload')
    except:
        pump_index = place_holder
