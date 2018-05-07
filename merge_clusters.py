import argparse
from common.tamr_api_methods import TamrAPI


def merge_clusters(host, protocol, port, username, password,
                   unified_dataset_name, cluster_name_field,
                   target_cluster_id, cluster_ids_to_merge):

    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.merge_clusters(unified_dataset_name=unified_dataset_name,
                                         cluster_name_field=cluster_name_field,
                                         target_cluster_id=target_cluster_id,
                                         cluster_ids_to_merge=cluster_ids_to_merge)
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
    parser.add_argument('--target_cluster_id', type=list, required=True,
                        help='ID of target cluster')
    parser.add_argument('--cluster_ids_to_merge', type=list, required=True,
                        help='List of cluster IDs to be merged')

    args = parser.parse_args()
    merge_clusters(host=args.host,
                   protocol=args.protocol,
                   port=args.port,
                   username=args.username,
                   password=args.password,
                   unified_dataset_name=args.unified_dataset_name,
                   cluster_name_field=args.cluster_name_field,
                   target_cluster_id=args.target_cluster_id,
                   cluster_ids_to_merge=args.cluster_ids_to_merge)
