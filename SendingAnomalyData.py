import pandas as pd
from datetime import datetime, timedelta
import json
from pprint import pprint
import requests
import matplotlib.pyplot as plt
import os


class MindSphereDectectAnomaly(object):

    def __init__(self, csv_file_name, csv_to_df_to_json_file=None, delete_columns=None):
        self.iot_timeseries = []
        self.multipart_message = ''
        self.df = pd.read_csv(csv_file_name).dropna(axis=1, how='all').dropna()
        if csv_to_df_to_json_file:
            with open(os.path.join(*csv_to_df_to_json_file), 'w') as out:
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
        self.df['TimeStamp'] = iso_8601_timestamp_list
        if delete_columns:
            self.df = self.df.drop(columns=delete_columns)
        print(self.df.head())
        print(self.df.columns)


    def convert_data_to_body(self, my_mindsphere_variable, category):
        create_model_body = []
        for each_time in range(self.df.shape[0]):
            one_data_point = {
                my_mindsphere_variable: self.df[category].iloc[each_time],
                '_time': self.df['TimeStamp'].iloc[each_time]
            }
            create_model_body.append(one_data_point)
        return create_model_body

    def create_data_model(self, body, technical_user_token, write_to_file=None, radius='0.1', min_num_pts_per_cluster='3'):
        url = "https://gateway.eu1.mindsphere.io/api/anomalydetection/v3/models"
        querystring = {"epsilon": str(radius), "minPointsPerCluster": str(min_num_pts_per_cluster)}
        payload = json.dumps(body)
        headers = {
            'Content-Type': "application/json",
            'X-XSRF-TOKEN': "1d934557-ed04-4ed2-88ff-22cef15462bb",
            'Cache-Control': "no-cache",
            'authorization': 'Bearer ' + technical_user_token
            }

        response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        if response.status_code == 201:
            response_body = response.json()
            self.id = response_body['id']
            self.variable = response_body['variables']
            if write_to_file:
                with open(os.path.join(*write_to_file), 'w') as out:
                    out.write(json.dumps(body, indent=4) + '\n')
        else:
            print(response.status_code)
            print(response.content)
            raise ValueError('Sending Model Failed!')

    def detect_anomaly(self, body, tech_token, anomaly_body=os.path.join('InBetweenSteps', 'anomaly_body.json')):
        url = "https://gateway.eu1.mindsphere.io/api/anomalydetection/v3/detectanomalies"
        querystring = {"modelID": self.id}
        payload = json.dumps(body)
        headers = {
            'Content-Type': "application/json",
            'X-XSRF-TOKEN': "1d934557-ed04-4ed2-88ff-22cef15462bb",
            'Cache-Control': "no-cache",
            'authorization': 'Bearer ' + tech_token
            }
        response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        response_body = response.json()
        if response.status_code == 200:
            anomaly_extent_list = []
            time_list = []
            for each_point in response_body:
                anomaly_extent_list.append(each_point['anomalyExtent'])
                time_list.append(each_point['_time'])
            self.df2 = pd.DataFrame({
                'anomalyExtent': anomaly_extent_list,
                '_time': time_list
            })
            with open(anomaly_body, 'w') as out:
                out.write(json.dumps(response_body, indent=4) + '\n')
        else:
            print(response.status_code)
            pprint(response.content)
            raise ValueError('Sending Test Failed!')

    @staticmethod
    def creating_new_dataframe_from_sub_dataframe(bigger_dataframe, bdf_key, subset_dataframe, sdf_key):
        return bigger_dataframe[bigger_dataframe[bdf_key].isin(subset_dataframe[sdf_key]).tolist()]

    def plot_curves(self, x1, y1, x2, y2, x3, y3):
        plt.figure(figsize=(10, 5))
        plt.title("X - y Scatter plot")

        plt.scatter(x1, y1, label='Sample to Test')
        plt.scatter(x2, y2, label='Anomaly')
        plt.scatter(x3, y3, label='Normal')
        plt.legend(loc="upper left")
        plt.grid(True)
        plt.show()


def creating_new_dataframe_from_sub_dataframe(bigger_dataframe, bdf_key, subset_dataframe, sdf_key):
    return bigger_dataframe[bigger_dataframe[bdf_key].isin(subset_dataframe[sdf_key]).tolist()]



if __name__ == '__main__':
    with open(os.path.join('TechnicalUserFolder', 'technicalUserToken.json'), 'r') as token_file:
        tech_token = json.loads(token_file.read())['tech_token']
    mda = MindSphereDectectAnomaly(os.path.join('MachineLearningReferenceData', 'turbine.csv'), ['InBetweenSteps', 'something.json'], ['Time'])
    body = mda.convert_data_to_body('Turbine Temp', 'Tb_mod_n')
    mda.create_data_model(body, tech_token, ['InBetweenSteps', 'model.json'])
    body2 = mda.convert_data_to_body('Turbine Temp', 'Tb_model')
    mda.detect_anomaly(body2, tech_token)
    mda.df['epoc'] = [datetime.strptime(each.split('.')[0], '%Y-%m-%dT%H:%M:%S').timestamp() for each in mda.df['TimeStamp']]
    mda.df.to_csv(os.path.join('InBetweenSteps', 'df.csv'))
    mda.df2['epoc'] = [datetime.strptime(each.split('.')[0], '%Y-%m-%dT%H:%M:%S').timestamp() for each in mda.df2['_time']]
    mda.df2.to_csv(os.path.join('InBetweenSteps', 'df2.csv'))

    df3 = creating_new_dataframe_from_sub_dataframe(mda.df, 'epoc', mda.df2, 'epoc')

    mda.plot_curves(mda.df['epoc'], mda.df['Tb_model'], df3['epoc'], df3['Tb_model'], mda.df['epoc'], mda.df['Tb_mod_n'])



