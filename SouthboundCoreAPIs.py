import requests
import json
from datetime import datetime
import time
import jwt
from pathlib import Path
import os


class Agent(object):

    def __init__(self, onboarding_json_file_location='onboarding.json', registration_json_file_location=None):
        '''
        This constructor will generate a registration file when given an onboarding file.
        If a registration file name is given, it will check if the file exist.
        If the registration file exist, it will validate it.
        If not, the constructor will save the registration under that name.
        If the registration file name is not givent, the file name will default to registration.json
        :param onboarding_json_file_location: The file needed from MindSphere to onboard a custom agent
        :param registration_json_file_location: The file name of the registration file
        '''
        print('Running init of Agent Class')
        self.check_for_valid_onboarding_access_token(onboarding_json_file_location)
        if registration_json_file_location:
            my_file = Path(registration_json_file_location)
            if my_file.is_file():
                self.check_for_valid_registration_access_token(registration_json_file_location)
            else:
                self.registering_an_agent_api(registration_json_file_location)
                self.check_for_valid_registration_access_token(registration_json_file_location)
        else:
            self.registering_an_agent_api()
            self.check_for_valid_registration_access_token()

    def registering_an_agent_api(self, rjfl='registration.json'):
        '''
        This method will create the registration token and save it with the text file name save in rjfl
        :param rjfl:
        :return: Boolean if the registration was successful or not
        '''
        url = self.base_url + "/api/agentmanagement/v3/register"
        payload = '{}'
        headers = {
            'content-type': 'application/json',
            'authorization': 'Bearer ' + self.iat
            }
        response = requests.request("POST", url, data=payload, headers=headers)
        print("Creating the Registration Access Token...")
        if response.status_code == 201:
            self.registration_json_file_location = rjfl
            print('Saving New Registration Info to: ', self.registration_json_file_location)
            with open(rjfl, 'w') as out:
                out.write(json.dumps(response.json(), indent=4) + '\n')
            print('New Registration Access Token was written to File! Success!')
            return True
        elif response.status_code == 401:
            print('Your device is likely already onboarded')
            print('You have to find your Registration Access Token')
            return True
        else:
            print('New Registration Access Token CANNOT be created')
            print(response.status_code)
            print(response.headers)
            print(response.content)
            return False

    def access_token_service_api(self, ajfl='authorization.json'):
        '''
        This method will create an authorization access token. It is good for one hour.
        :param ajfl: Name of authorization access token name
        :return: boolean depending on success of authorization token
        '''
        timestamp_in_local_time = int(str(time.mktime(datetime.now().timetuple())).split('.')[0])
        twenty_four_hours_from_now = timestamp_in_local_time + 24 * 60 * 60
        print(datetime.fromtimestamp(timestamp_in_local_time))
        self.authorization_access_expiration = timestamp_in_local_time+ 60 * 60
        print('Authorization Access Token will expire at: ' + str(datetime.fromtimestamp(self.authorization_access_expiration)))
        url = self.base_url + '/api/agentmanagement/v3/oauth/token'
        pre_encode = {
            'iss': self.registration_client_id,
            'sub': self.registration_client_id,
            'aud': "southgate",
            'iat': timestamp_in_local_time,
            'nbf': timestamp_in_local_time,
            'exp': twenty_four_hours_from_now,
            'schemas': ["urn:siemens:mindsphere:v1"],
            'ten': self.tenant,
            'jti': '607ae222-01b9-4f15-81d4-33f73663da5e'
        }
        payload = '&'.join([
            'grant_type=client_credentials',
            'client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_assertion=' + jwt.encode(pre_encode, self.registration_client_secret).decode('utf-8')
        ])
        headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }

        response = requests.request("POST", url, data=payload, headers=headers)
        if response.status_code == 200:
            with open(ajfl, 'w') as auth_file:
                authorization_object = json.loads(response.content.decode('utf-8'))
                authorization_object['authorization_access_expiration'] = self.authorization_access_expiration
                auth_file.write(json.dumps(authorization_object, indent=4) + '\n')
            self.authorization_access_token = authorization_object['access_token']
            print('Authorization Access Token created and saved to file')
            return True
        else:
            print(response.status_code)
            print(response.content)
            raise ValueError('Authorization Access Token FAILED')

    def mindsphere_exchange_api(self, multipart_outer_boundary, multipart_file_location='reference_multipart.txt', ):
        '''
        Sends a multipart API to MindSphere to Upload Data. If the multipart_file_location is the true multipart text,
        it will be sent directly to MindSphere. If it is a file name, it will read that file and then send it to MindSphere.
        :param multipart_outer_boundary: string, boundary text for the multipart message
        :param multipart_file_location: string, multipart or file name that ends with txt
        :return: boolean, if operation was successful
        '''
        headers = {
            'content-type': 'multipart/mixed;charset=utf-8; boundary=' + multipart_outer_boundary,
            'authorization': 'Bearer ' + self.authorization_access_token
        }
        if multipart_file_location.split('.')[-1] == 'txt':
            with open(multipart_file_location, 'rb') as out:
                files = {'file': out.read()}
        else:
            files = {'file': multipart_file_location}
        url = self.base_url + "/api/mindconnect/v3/exchange"
        response = requests.request("POST", url, files=files, headers=headers)
        if response.status_code == 200:
            print('MindSphere has ACCEPTED your data upload!')
        elif response.status_code == 413:
            print('Multipart file size exceeds maximum size allowed')
        elif response.status_code == 429:
            print('You are being throttled. Waiting 30 seconds before retrying')
            time.sleep(30)
        else:
            print('================')
            print(response.status_code)
            print(response.content)
            print('================')

    def check_for_valid_onboarding_access_token(self, ojfl='onboarding.json'):
        print("Checking for a valid onboarding_access_token")
        my_file = Path(ojfl)
        if my_file.is_file():
            print(ojfl)
            with open(ojfl) as onboard_file:
                onboarding_object = json.loads(onboard_file.read())
                if 'expiration' in onboarding_object:
                    print('Your expiration time to onboard your agent: ' + onboarding_object['expiration'])
                    if all(k in onboarding_object['content'] for k in ('baseUrl', 'tenant', 'iat')):
                        self.base_url = onboarding_object['content']['baseUrl']
                        self.tenant = onboarding_object['content']['tenant']
                        self.iat = onboarding_object['content']['iat']
                        print('Onboarding File is Valid!')
                        return True
                    else:
                        print('Onboarding File is CORRUPT.')
                        return False
        else:
            raise ValueError("Please provide correct onboarding file location and COPY/PASTE Correctly")

    def check_for_valid_registration_access_token(self, rjfl='registration.json'):
        my_rjfl = Path(rjfl)
        if my_rjfl.is_file():
            with open(rjfl) as registration_file:
                registration_object = json.loads(registration_file.read())
                if all(k in registration_object for k in
                       ('client_id', 'client_secret', 'registration_access_token', 'client_secret_expires_at', 'registration_client_uri')):
                    self.registration_client_id = registration_object['client_id']
                    self.registration_client_secret = registration_object['client_secret']
                    self.registration_access_token = registration_object['registration_access_token']
                    self.registration_client_secret_expires_at = registration_object['client_secret_expires_at']
                    self.registration_client_uri = registration_object['registration_client_uri']
                    print("Registration Access Token will expire on: " + str(
                        datetime.utcfromtimestamp(self.registration_client_secret_expires_at)))
                    print("Current time is: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    if datetime.utcfromtimestamp(self.registration_client_secret_expires_at) > datetime.now():
                        print("Registration Access Token is STILL valid")
                        self.registration_json_file_location = rjfl
                        print("I highly recommend refreshing your Registration Access Token")
                        return True
                    else:
                        print("Registration Access Token has EXPIRED. You must now get the original onboarding token!")
                        return False
                else:
                    raise ValueError('Registration Access Token is INVALID')
        else:
            raise ValueError("Please provide correct onboarding file location")

    def check_for_valid_authorization_access_token(self, ajfl='authorization.json'):
        my_ajfl = Path(ajfl)
        if my_ajfl.is_file():
            with open(ajfl) as authorize_file:
                authorization_object = json.loads(authorize_file.read())
                if all(k in authorization_object for k in ('access_token', 'authorization_access_expiration', 'jti')):
                    self.authorization_access_expiration = authorization_object['authorization_access_expiration']
                    if datetime.utcfromtimestamp(self.authorization_access_expiration) > datetime.now():
                        print("Authorization Access Token will expire on: " + str(
                            datetime.utcfromtimestamp(self.authorization_access_expiration)))
                        print('You have a valid Authorization Access Token')
                        return True
                    else:
                        print('Your Authorization Access Token is EXPIRED')
                        return False
                else:
                    raise ValueError('Your Authorization Access Token is INVALID')

    def refreshing_registration_access_token_api(self, rjfl='registration.json'):
        payload2 = {'client_id': self.registration_client_id}
        headers2 = {
            'content-type': 'application/json',
            'authorization': 'Bearer ' + self.registration_access_token
            }
        response = requests.request("PUT", self.registration_client_uri, json=payload2, headers=headers2)
        update_response = response.json()
        if response.status_code == 200:
            with open(rjfl, 'w') as out:
                out.write(json.dumps(update_response, indent=4) + '\n')
            print('Update, refresh, or rolling of Registration Access Token was a success.')
            print('Update, refresh, or rolling of Registration Access Token was written to File! Success!')
            self.registration_client_secret = update_response['client_secret']
            self.registration_access_token = update_response['registration_access_token']
            self.registration_client_secret_expires_at = update_response['client_secret_expires_at']
            print("Registration Access Token will expire on: " + str(
                datetime.utcfromtimestamp(self.registration_client_secret_expires_at)))
            return True
        else:
            print('Update, refresh, or rolling of the Registration Token failed')
            print(response.status_code)
            print(response.json())
            return False

    @staticmethod
    def convert_datetime_of_now_to_human_readable_text():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def convert_timestamp_to_human_readable_datetime_object(timestamp):
        return datetime.fromtimestamp(timestamp)

    @staticmethod
    def convert_timestamp_to_utc(timestamp):
        return datetime.utcfromtimestamp(timestamp)


if __name__ == "__main__":
    an_asset = Agent(os.path.relpath('SouthBoundTokens/Initial.json'),
                     os.path.relpath('SouthBoundTokens/Registration.json'))
    an_asset.access_token_service_api(os.path.relpath('SouthBoundTokens/Access.json'))
