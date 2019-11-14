from ConstantUpload import ConstantUpload
from pprint import pprint
from time import sleep
import pandas as pd
import json
import os

data_source = '1551302589557'
n=1

data_point_id_lookup_table = {
    'PressureOut': '1551302290959',
    'PressueIn': '1551302329220',
    'StuffingBoxTemp': '1551302223661',
    'Passage': '1551302273734',
    'MotorCurrent': '1551302259109'
}

onboarding_json_file_location = os.path.relpath('SouthBoundTokens/Initial.json')
registration_json_file_location = os.path.relpath('./SouthBoundTokens/Registration.json')
authorization_json_file_location = os.path.relpath('./SouthBoundTokens/Access.json')
my_agent = ConstantUpload(onboarding_json_file_location,
                          registration_json_file_location,
                          authorization_json_file_location)




df = pd.read_csv(os.path.relpath('BulkUploadReferenceData/PumpData/649D62D0F97540A99F3876AE26F30B0B_XTools (00).csv'))

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