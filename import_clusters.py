import argparse
import csv
from common.tamr_api_methods import TamrAPI


def get_cluster_json(csv_filepath, json_filepath,
                     input_dataset_name, unified_dataset_name,
                     unique_id_column_name, tamr_cluster_id_col,
                     encoding='utf-8'):

    """
    Takes a DataFrame with with record and cluster ids as well as
    names of the input dataset and unified dataset and creates
    a file of newline-separated JSON fragments to fed into the
    Tamr dataset/clusters/{dataset}/import API
    """
    with open(csv_filepath, 'r', encoding=encoding) as infile:
        with open(json_filepath, 'w') as outfile:
            csv_reader = csv.DictReader(infile)
            for i, row in enumerate(csv_reader):
                outrow = '"datasetName": "{0}", "recordId": "{1}", ' \
                         '"originDatasetName": "{2}", ' \
                         '"originRecordId": "{1}", ' \
                         '"clusterId": "{3}"'.format(unified_dataset_name,
                                                     row[unique_id_column_name],
                                                     row[tamr_cluster_id_col], )
                outrow = '{' + outrow + '}'
                outfile.write(outrow + '\n')
    return json_filepath


def import_clusters(host, protocol, port, username, password,
                    unified_dataset_name, cluster_name_field,
                    recipe_id, csv_filepath, json_filepath,
                    input_dataset_name, unique_id_column_name,
                    tamr_cluster_id_col, encoding, create_json=True):

    if create_json:
        json_filepath = get_cluster_json(csv_filepath, json_filepath,
                                         input_dataset_name, unified_dataset_name,
                                         unique_id_column_name, tamr_cluster_id_col,
                                         encoding=encoding)
    with TamrAPI(host, protocol, port, username, password) as api:
        response = api.import_clusters(unified_dataset_name=unified_dataset_name,
                                       cluster_name_field=cluster_name_field,
                                       recipe_id=recipe_id,
                                       json_filepath=json_filepath)
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
    parser.add_argument('--cluster_name_field', type=str, required=True,
                        help='Name of the column selected to represent cluster name')
    parser.add_argument('--recipe_id', type=str, required=True,
                        help='DEDUP Recipe ID Number')
    parser.add_argument('--csv_filepath', type=str, required=False,
                        help='CSV filepath with cluster memberships. Can be omitted if create_json is False')
    parser.add_argument('--json_filepath', type=str, required=True,
                        help='JSON filepath with cluster memberships')
    parser.add_argument('--input_dataset_name', type=str, required=True,
                        help='Name of input dataset or origin_source_id')
    parser.add_argument('--unique_id_column_name', type=str, required=True,
                        help='Name of primary key field for input dataset')
    parser.add_argument('--tamr_cluster_id_col', type=str, required=True,
                        help='Name of field containing cluster IDs')
    parser.add_argument('--encoding', type=str, required=False, default='utf-8',
                        help='Encoding, default is utf-8')
    parser.add_argument('--create_json', type=bool, required=False, default=True,
                        help='Create JSON file with cluster membership, default is True')

    args = parser.parse_args()
    import_clusters(host=args.host,
                    protocol=args.protocol,
                    port=args.port,
                    username=args.username,
                    password=args.password,
                    unified_dataset_name=args.unified_dataset_name,
                    cluster_name_field=args.cluster_name_field,
                    recipe_id=args.recipe_id,
                    csv_filepath=args.csv_filepath,
                    json_filepath=args.json_filepath,
                    input_dataset_name=args.input_dataset_name,
                    unique_id_column_name=args.unique_id_column_name,
                    tamr_cluster_id_col=args.tamr_cluster_id_col,
                    encoding=args.encoding,
                    create_json=args.create_json)
