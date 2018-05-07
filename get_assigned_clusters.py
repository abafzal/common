import argparse
from common.tamr_api_methods import TamrAPI


def get_assigned_clusters(host, protocol, port, username, password):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.get_assigned_clusters()
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

    args = parser.parse_args()
    get_assigned_clusters(host=args.host,
                   protocol=args.protocol,
                   port=args.port,
                   username=args.username,
                   password=args.password)
