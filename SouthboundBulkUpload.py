from SouthboundCoreAPIs import Agent
import json
from datetime import datetime, timedelta
from random import choice
import pandas as pd
import os


class AgentDataUpload(Agent):

    def __init__(self, csv_file_name, onboarding_json_file_location, write_to_file=None,
                 registration_json_file_location=None, authorization_json_file_location=None, delete_columns=None):
        super(AgentDataUpload, self).__init__(onboarding_json_file_location, registration_json_file_location)
        self.access_token_service_api(registration_json_file_location)
        self.refreshing_registration_access_token_api(registration_json_file_location)
        self.access_token_service_api(authorization_json_file_location)
        self.iot_timeseries = []
        self.multipart_message = ''
        self.df = pd.read_csv(csv_file_name).dropna(axis=1, how='all').dropna()
        if write_to_file:
            with open(write_to_file, 'w') as out:
                out.write(json.dumps(self.df.to_dict(), indent=4) + '\n')
        iso_8601_timestamp_list = []
        print(self.df.head())
        if all(k in self.df for k in ('Date', 'Time')):
            for each_date, each_time in zip(self.df['Date'].tolist(), self.df['Time'].tolist()):
                iso_8601_timestamp_list.append(str(datetime.strptime(each_date + ' ' + each_time, '%m/%d/%Y %H:%M:%S').isoformat()) + 'Z')
            self.df = self.df.drop(columns=['Date', 'Time'])
        elif 'DateTime' in self.df:
            for each_datetime in self.df['DateTime'].tolist():
                iso_8601_timestamp_list.append(str(datetime.strptime(each_datetime, '%m/%d/%Y %H:%M:%S').isoformat()) + 'Z')
            self.df = self.df.drop(columns=['DateTime'])
        else:
            now = datetime.utcnow()
            size = len(self.df.index)
            for each_key in range(size):
                iso_8601_timestamp_list.append(
                    str((now - timedelta(hours=int(size) - each_key)).isoformat()) + 'Z')
        self.df['TimeSeries'] = iso_8601_timestamp_list
        if delete_columns:
            self.df = self.df.drop(columns=delete_columns)

    def create_iot_timeseries(self, data_point_lookup_table, file_to_examine=''):
        sub_df = self.df.drop('TimeSeries', axis=1)
        print(sub_df.columns)
        for index_segment_time, value_segment_time in enumerate(self.df['TimeSeries']):
            values_list = []
            for each_time_series_value in sub_df.columns:
                data_column = sub_df[each_time_series_value].tolist()
                values_list.append({
                    'dataPointId': str(data_point_lookup_table[each_time_series_value]),
                    'value': data_column[index_segment_time],
                    'qualityCode': '0'
                })
            self.iot_timeseries.append({
                'timestamp': value_segment_time,
                'values': values_list
            })
        if file_to_examine:
            print('JSON Data Input Stored in: ', file_to_examine)
            with open(file_to_examine, 'w') as out:
                out.write(json.dumps(self.iot_timeseries, indent=4) + '\n')

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


if __name__ == '__main__':
    data_point_id_lookup_table = {
        'CO(GT)': '1',
        'PT08.S1(CO)': '2',
        'NMHC(GT)': '3',
        'C6H6(GT)': '4',
        'PT08.S2(NMHC)': '5',
        'NOx(GT)': '6',
        'PT08.S3(NOx)': '7',
        'NO2(GT)': '8',
        'PT08.S4(NO2)': '9',
        'PT08.S5(O3)': '10',
        'T': '11',
        'RH': '12',
        'AH': '13'
    }
    an_agent = AgentDataUpload(os.path.join('BulkUploadReferenceData', 'Dataset_AirQualityUCI.csv'),
                               os.path.join('SouthBoundTokens', 'Initial.json'),
                               os.path.join('InBetweenSteps', 'data_output.json'),
                               os.path.join('SouthBoundTokens', 'Registration.json'),
                               os.path.join('SouthBoundTokens', 'Authorization.json'))

    # data_point_id_lookup_table = {
    #     'Tb_model': '1',
    #     'Tb_mod_n': '2',
    # }
    # an_agent = AgentDataUpload(os.path.join('MachineLearningReferenceData', 'turbine.csv'),
    #                            os.path.join('SouthBoundTokens', 'raspberryPiZeroAgentOnboarding.json'),
    #                            os.path.join('InBetweenSteps', 'data_output.json'),
    #                            os.path.join('SouthBoundTokens', 'raspberryPiZeroAgentRegistration.json'),
    #                            os.path.join('SouthBoundTokens', 'raspberryPiZeroAgentAuthorization.json'), ['Time'])

    an_agent.create_iot_timeseries(data_point_id_lookup_table, os.path.join('InBetweenSteps', 'create_timeseries_entry.json'))
    multipart_boundary = an_agent.write_multipart(1536248741433, os.path.join('InBetweenSteps', 'multipart.txt'))
    an_agent.mindsphere_exchange_api(multipart_boundary, an_agent.multipart_message)