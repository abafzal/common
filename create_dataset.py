import os
import errno
import argparse
from common.tamr_api_methods import TamrAPI


def is_valid_file(filename):
    if not os.path.isfile(filename):
        raise OSError(
            errno.ENOENT, os.strerror(errno.ENOENT), filename)
    else:
        return filename


def create_dataset(host, protocol, port, username, password,
                   dataset_name, csv_filepath,
                   id_field, description):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.create_dataset(dataset_name=dataset_name,
                                      file_path=is_valid_file(csv_filepath),
                                      id_field=id_field,
                                      delimiter=',',
                                      encoding='utf-8',
                                      description=description)
    return response


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, required=True,
                        help='Unify host')
    parser.add_argument('--protocol', type=str, default='http',
                        help='Connection protocol, defaults to http')
    parser.add_argument('--port', type=str, default='9100',
                        help='Unify port, defaults to 9100')
    parser.add_argument('--username', type=str, required=True,
                        help='Unify Username')
    parser.add_argument('--password', type=str, required=True,
                        help='Unify Password')
    parser.add_argument('--csv_filepath', type=str, required=True,
                        help='Path to CSV')
    parser.add_argument('--dataset_name', type=str, required=True,
                        help='Dataset Name')
    parser.add_argument('--description', type=str,
                        help='Dataset description')
    parser.add_argument('--id_field', type=str, default=None,
                        help='ID Field')
    args = parser.parse_args()
    create_dataset(host=args.host,
                   protocol=args.protocol,
                   port=args.port,
                   username=args.username,
                   password=args.password,
                   dataset_name=args.dataset_name,
                   csv_filepath=args.csv_filepath,
                   description=args.description,
                   id_field=args.id_field)
