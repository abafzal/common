import argparse
from common.tamr_api_methods import TamrAPI


def assign_clusters(host, protocol, port, username, password,
                    unified_dataset_name, usernames,
                    cluster_ids):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.assign_clusters(unified_dataset_name=unified_dataset_name,
                                       usernames=usernames,
                                       cluster_ids=cluster_ids)
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
    parser.add_argument('--usernames', type=list, required=True,
                        help='List of usernames to assign clusters to')
    parser.add_argument('--cluster_ids', type=list, required=True,
                        help='List of cluster IDs')

    args = parser.parse_args()
    assign_clusters(host=args.host,
                    protocol=args.protocol,
                    port=args.port,
                    username=args.username,
                    password=args.password,
                    unified_dataset_name=args.unified_dataset_name,
                    usernames=args.usernames,
                    cluster_ids=args.cluster_ids)
