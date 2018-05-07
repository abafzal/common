import argparse
from common.tamr_api_methods import TamrAPI


def split_cluster(host, protocol, port, username, password,
                   unified_dataset_name, cluster_name_field, 
                   json_file_path):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.split_cluster(unified_dataset_name=unified_dataset_name,
                                     cluster_name_field=cluster_name_field, 
                                     json_file_path=json_file_path)
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
    parser.add_argument('--unified_dataset_name', type=str, required=True,
                        help='Name of the unified dataset')
    parser.add_argument('--cluster_name_field', type=list, required=True,
                        help='Name of the column selected to represent cluster name')
    parser.add_argument('--json_file_path', type=str, required=True,
                        help='Filepath to newline-separated JSON')


    args = parser.parse_args()
    split_cluster(host=args.host,
                   protocol=args.protocol,
                   port=args.port,
                   username=args.username,
                   password=args.password,
                   unified_dataset_name=args.unified_dataset_name,
                   cluster_name_field=args.cluster_name_field, 
                   json_file_path=args.json_file_path)
