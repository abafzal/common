import os
import errno
import argparse
import datetime
from common.tamr_api_methods import TamrAPI


def is_valid_file(filename):
    if not os.path.isfile(filename):
        raise OSError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename)
    else:
        return filename


def truncate_load(host, protocol, port, username, password, dataset_name, csv_filepath, id_field, create_json=True):

    start = datetime.datetime.now()

    csv_filepath = is_valid_file(csv_filepath)

    # Truncate dataset. 
    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.truncate_dataset(dataset_name)
    print(response.json())

    # Upload records. 
    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.update_dataset(dataset_name=dataset_name,
                                      file_name=csv_filepath,
                                      id_field=id_field,
                                      create_json=create_json)
    print(response.json())

    end = datetime.datetime.now()
    duration = round((end - start).seconds / 60, 3)
    print('Completed in {duration} minutes'.format(duration=duration))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, required=True,
                        help='Unify host')
    parser.add_argument('--protocol', type=str, default='http',
                        help='Connection protocol, defaults to http')
    parser.add_argument('--port', type=str, default=9100,
                        help='Unify Port, defaults to 9100')
    parser.add_argument('--username', type=str, required=True,
                        help='Unify Username')
    parser.add_argument('--password', type=str, required=True,
                        help='Unify Password')
    parser.add_argument('--csv_filepath', type=str, required=True,
                        help='Path to CSV')
    parser.add_argument('--dataset_name', type=str, required=True,
                        help='Dataset Name')
    parser.add_argument('--id_field', type=str,
                        help='ID field')
    parser.add_argument('--create_json', type=bool, default=True,
                        help='false if latest JSON file already exists. Default is true')

    args = parser.parse_args()
    truncate_load(host=args.host,
                  protocol=args.protocol,
                  port=args.port,
                  username=args.username,
                  password=args.password,
                  dataset_name=args.dataset_name,
                  csv_filepath=args.csv_filepath,
                  id_field=args.id_field,
                  create_json=args.create_json)
