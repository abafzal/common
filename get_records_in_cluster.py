import argparse
from common.tamr_api_methods import TamrAPI


def get_records_in_cluster(host, protocol, port, username, password,
                   unified_dataset_name, cluster_id, sort_by_spend='false', limit=100):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.get_records_in_cluster(unified_dataset_name=unified_dataset_name,
                                              cluster_id=cluster_id, 
                                              sort_by_spend=sort_by_spend, 
                                              limit=limit)
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
    parser.add_argument('--cluster_id', type=str, required=True,
                        help='Cluster ID')
    parser.add_argument('--sort_by_spend', type=str, default='false',
                        help='Sort by spend, default is false')
    parser.add_argument('--limit', type=int, default=100,
                        help='Record limit, default is 100')


    args = parser.parse_args()
    get_records_in_cluster(host=args.host,
                   protocol=args.protocol,
                   port=args.port,
                   username=args.username,
                   password=args.password,
                   unified_dataset_name=args.unified_dataset_name,
                   cluster_id=args.cluster_id,
                   sort_by_spend=args.sort_by_spend,
                   limit=args.limit)
