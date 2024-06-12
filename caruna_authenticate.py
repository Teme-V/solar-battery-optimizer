import sys
import configparser
import datetime
from pycaruna import Authenticator

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('.secrets.txt')  # Update the path to read secrets.txt correctly

    username = config['caruna']['CARUNA_USER']
    password = config['caruna']['CARUNA_PASS']

    timestamp = datetime.datetime.now().isoformat()

    if username is None or password is None:
        raise Exception('CARUNA_USER and CARUNA_PASS must be defined')

    authenticator = Authenticator(username, password)
    login_result = authenticator.login()

    # Update the CARUNA_TOKEN and CARUNA_TOKEN_TIMESTAMP
    config['caruna']['CARUNA_CUSTOMER_ID'] = login_result['user']['ownCustomerNumbers'][0]
    config['caruna']['CARUNA_TOKEN'] = login_result['token']
    config['caruna']['CARUNA_TOKEN_TIMESTAMP'] = timestamp
 
    # Write the updated configuration back to secrets.txt
    with open('.secrets.txt', 'w') as configfile:
        config.write(configfile)
