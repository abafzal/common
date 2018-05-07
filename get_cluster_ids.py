import argparse
from common.tamr_api_methods import TamrAPI

def get_cluster_ids(host, protocol, port, username, password,
                    unified_dataset_name, query='', offset='',
                    limit='', sort='', cluster_ids='', name=''):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.get_cluster_ids(unified_dataset_name=unified_dataset_name,
                                       query=query,
                                       offset=offset,
                                       limit=limit,
                                       sort=sort,
                                       cluster_ids=cluster_ids,
                                       name=name)
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
    parser.add_argument('--query', type=str, required=False,
                        help='Tamr Query', default='')
    parser.add_argument('--offset', type=str, required=False,
                        help='Offset', default='')
    parser.add_argument('--limit', type=str, required=False,
                        help='Limit', default='')
    parser.add_argument('--sort', type=list, required=False,
                        help='Sort Order [asc] or [desc]', default='')
    parser.add_argument('--cluster_ids', type=list, required=False,
                        help='Cluster IDs', default='')
    parser.add_argument('--name', type=str, required=False,
                        help='Cluster Name', default='')

    args = parser.parse_args()
    get_cluster_ids(host=args.host,
                    protocol=args.protocol,
                    port=args.port,
                    username=args.username,
                    password=args.password,
                    unified_dataset_name=args.unified_dataset_name,
                    query=args.query,
                    offset=args.offset,
                    limit=args.limit,
                    sort=args.sort,
                    cluster_ids=args.cluster_ids,
                    name=args.name)
