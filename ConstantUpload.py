from SouthboundCoreAPIs import Agent
from datetime import datetime
from random import randint, choice, normalvariate
from pprint import pprint
from time import sleep
# import os
import json
import socket
from pathlib import Path


class ConstantUpload(Agent):

    def __init__(self, onboarding_json_file_location, registration_json_file_location, authorization_json_file_location=None):
        self.registration_json_file_location = registration_json_file_location
        self.data_to_dict = {}
        self.iot_timeseries = []
        if authorization_json_file_location:
            self.authorization_json_file_location = authorization_json_file_location
        else:
            self.authorization_json_file_location = 'authorization.json'
        super(ConstantUpload, self).__init__(onboarding_json_file_location, self.registration_json_file_location)
        self.check_for_valid_registration_access_token(self.registration_json_file_location)
        self.refreshing_registration_access_token_api(self.registration_json_file_location)
        self.access_token_service_api(self.authorization_json_file_location)
        self.check_for_valid_authorization_access_token(self.authorization_json_file_location)


    def create_an_entry_to_iot_timeseries(self, data_point_lookup_table):
        values_list = []
        now = str(datetime.utcnow().isoformat()) + 'Z'
        for data_entry in self.data_to_dict:
            values_list.append({
                'dataPointId': str(data_point_lookup_table[data_entry]),
                'value': self.data_to_dict[data_entry],
                'qualityCode': '0'
            })
        self.iot_timeseries.append({
            'timestamp': now,
            'values': values_list
        })

    def write_multipart(self, configuration_id, file_name=''):
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
            json.dumps(self.iot_timeseries),
            '',
            '--' + multipart_inner_boundary + '--',
            '--' + multipart_outer_boundary + '--'
        ]
        multipart_message = '\n'.join(multipart_message_list)
        self.multipart_message = '\r\n'.join(multipart_message_list)
        if file_name:
            with open(file_name, 'w') as out:
                out.write(multipart_message)
            print('Multipart File Stored in: ' + file_name)
        return multipart_outer_boundary

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


if __name__ == '__main__':
    random_wait = randint(0, 10)
    data_source = '1553714837109'
    data_point_id_lookup_table = {
    'Voltage': '1553714765904',
    'OuputPower': '1553714723057',
    'WindSpeed': '1553714701039',
    'RotSpeed': '1553714616292',

    }
    an_agent = ConstantUpload(Path('SouthBoundTokens/Initial2.json'),
                              Path('SouthBoundTokens/Registration2.json'),
                              Path('SouthBoundTokens/Access2.json'))

    while True:
        # try:
            an_agent.data_to_dict = {
                'Voltage': normalvariate(110, .5),
                'OuputPower': normalvariate(225, .5),
                'WindSpeed': normalvariate(22, .5),
                'RotSpeed': normalvariate(5, .5),
                # 'Speed': randint(3,7),
                # 'Afraid': True,
            }
            an_agent.create_an_entry_to_iot_timeseries(data_point_id_lookup_table)
            # multipart_boundary = an_agent.write_multipart('543', os.path.join('InBetweenSteps', 'multipartContinuous.txt'))
            multipart_boundary = an_agent.write_multipart(data_source)
            pprint(an_agent.iot_timeseries)
            an_agent.mindsphere_exchange_api(multipart_boundary, an_agent.multipart_message)
            an_agent.check_every_token()
            an_agent.iot_timeseries = []
            if random_wait:
                sleep(random_wait)
        # except:
        #     print('Upload UNSUCCESSFUL! Will Try to Resend Unsent Data')
        #     print('When failed, it is usually because of network issues')
        #     print('It will reattempt because self.iot_time series does NOT get clear')

