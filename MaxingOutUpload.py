from SouthboundCoreAPIs import Agent
from datetime import datetime
from datetime import timedelta
from random import choice
import json
import os
import pandas as pd
import numpy as np
from pprint import pprint


class ContinuousMaxUploaded(Agent):

    def __init__(self, onboarding_json_file_location, registration_json_file_location, authorization_json_file_location=None):
        self.registration_json_file_location = registration_json_file_location
        self.authorization_json_file_location = authorization_json_file_location
        self.data_to_dict = {}
        self.iot_timeseries = []
        self.values_dict = {}
        if authorization_json_file_location:
            self.authorization_json_file_location = authorization_json_file_location
        else:
            self.authorization_json_file_location = 'authorization.json'
        super(ContinuousMaxUploaded, self).__init__(onboarding_json_file_location, self.registration_json_file_location)
        self.check_for_valid_registration_access_token(self.registration_json_file_location)
        self.refreshing_registration_access_token_api(self.registration_json_file_location)
        self.check_for_valid_authorization_access_token(self.authorization_json_file_location)
        self.access_token_service_api(self.authorization_json_file_location)

    def check_every_token(self):
        self.now_ish = int(str(datetime.now().timestamp()).split('.')[0])
        if self.registration_client_secret_expires_at < (self.now_ish + 60):
            print('Updating RAT')
            self.refreshing_registration_access_token_api(self.registration_json_file_location)
        else:
            print('RAT', self.registration_client_secret_expires_at - (self.now_ish + 60))
        if self.authorization_access_expiration < (self.now_ish + 60):
            print('Updating AAT')
            self.access_token_service_api(self.authorization_json_file_location)
        else:
            print('AAT', self.authorization_access_expiration - (self.now_ish + 60))

    def create_data_values_with_dataframe(self, data_frame):
        self.size = len(data_frame.index)
        for each_df_column in data_frame:
            values_list = []
            for each_value in data_frame[each_df_column]:
                values_list.append({
                    'dataPointId': str(data_point_id_lookup_table[each_df_column]),
                    'value': each_value,
                    'qualityCode': '0'
                })
            self.values_dict[each_df_column] = values_list
        return self.values_dict

    def create_time_for_given_size(self, data_frame):
        time_stamp_list = []
        for each_index, each_value in enumerate(data_frame.index):
            time_stamp_list.append(str((now - timedelta(seconds=int(self.size) - each_index)).isoformat()) + 'Z')
        return time_stamp_list

    def create_time_series_data(self, values_dict):
        big_value_list = []
        for each_entry in range(self.size):
            big_value_lista = []
            for each_column in values_dict:
                big_value_lista.append(values_dict[each_column][each_entry])
            big_value_list.append(big_value_lista)
        return big_value_list

    def create_full_time_series(self, values_in_mindsphere_format):
        iot_timeseries = []
        for each_entry in range(self.size):
            iot_timeseries.append({
                'timestamp': timestamp_list[each_entry],
                'values': values_in_mindsphere_format[each_entry]
            })
        return iot_timeseries

    def create_multipart(self, configuration_id, multipart_message_location=None):
        configuration = {
            "type": "item",
            "version": "1.0",
            "payload": {
                "type": "standardTimeSeries",
                "version": "1.0",
                "details": {
                    "configurationId": str(configuration_id)
                }
            }
        }
        letters_and_numbers = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        multipart_outer_boundary = ''.join([choice(letters_and_numbers) for x in range(22)])
        generated_word = ''.join([choice(letters_and_numbers) for x in range(22)])
        while multipart_outer_boundary == generated_word:
            generated_word = ''.join([choice(letters_and_numbers) for x in range(22)])
        multipart_inner_boundary = generated_word
        multipart_message_list = [
            '--' + multipart_outer_boundary,
            'Content-Type: multipart/related;boundary=' + multipart_inner_boundary,
            '',
            '--' + multipart_inner_boundary,
            'Content-Type: application/vnd.siemens.mindsphere.meta+json'
            '',
            '',
            # json.dumps(configuration, indent=4),
            json.dumps(configuration),
            '',
            '--' + multipart_inner_boundary,
            'Content-Type: application/json'
            '',
            '',
            # json.dumps(self.iot_timeseries, indent=4),
            json.dumps(iot_timeseries),
            '',
            '--' + multipart_inner_boundary + '--',
            '--' + multipart_outer_boundary + '--'
        ]
        self.multipart_message = '\r\n'.join(multipart_message_list)
        if multipart_message_location:
            multipart_message = '\n'.join(multipart_message_list)
            with open(multipart_message_location, 'w') as out:
                out.write(multipart_message)
            print('Multipart File Stored in: ' + multipart_message_location)
        return multipart_outer_boundary


if __name__ == '__main__':
    config_id = '1566855063432'
    data_point_id_lookup_table = {
        'RandomInt': '1566854896046'
    }
    an_agent = ContinuousMaxUploaded(os.path.join('SouthBoundTokens', 'MaxOnboarding.json'),
                                     os.path.join('SouthBoundTokens', 'MaxRegistration.json'),
                                     os.path.join('SouthBoundTokens', 'MaxAuthorization.json'))

    # count = 90700
    count = 90700
    # while True:
    count = count + 1
    an_agent.check_every_token()
    df = pd.DataFrame(np.random.randint(0, 9, [count, 1]), columns=['RandomInt'])
    print('the Count is', count)
    print(df.head())
    now = datetime.utcnow()

    values_dict2 = an_agent.create_data_values_with_dataframe(df)
    timestamp_list = an_agent.create_time_for_given_size(df)
    big_value_list = an_agent.create_time_series_data(values_dict2)
    iot_timeseries = an_agent.create_full_time_series(big_value_list)
    pprint(iot_timeseries[:5])

    # file_name = os.path.join('InBetweenSteps', 'MaxingOutUpload.txt')
    # multipart_outer_boundary = an_agent.create_multipart(configuration_id, file_name)
    mob = an_agent.create_multipart(config_id)
    an_agent.mindsphere_exchange_api(mob, an_agent.multipart_message)
    an_agent.iot_timeseries = []

