import os
import csv
import json
import time
import tamrapi
import logging
import subprocess


def check_status(success_code=200, silent=False):
    """
    Checks response of API request for status code. If status code
    does not return the specified success_code, it throws an error.
    :param success_code: Response code of the API call, default is 200
    :param silent: Disables logging if True, default is False
    :return: requests.Response object
    """

    def decorator(api_call_function):

        def wrapper(*args, **kwargs):
            response = api_call_function(*args, **kwargs)
            try:
                assert response.status_code == success_code
                if not silent:
                    logging.info("Called %s %s",
                                 response.request.method, response.request.url)
            # TODO: Do not use bare except
            except:
                logging.info("Called %s %s",
                             response.request.method,
                             response.request.url)
                logging.error("ERROR: Status code = {0}".format(response.status_code))
                logging.error("Request body:")
                logging.error(response.request.body)
                logging.error("Response:")
                logging.error(json.dumps(response.text))
                raise
            return response

        return wrapper

    return decorator


class TamrAPI(object):
    def __init__(self,
                 host, protocol, port,
                 user, pw):
        self.host = host
        self.port = port
        self.user = user
        self.pw = pw
        self.api_url = '/api'
        self.protocol = protocol

        if self.protocol.lower() == 'https':
            assert self.cert is not None, "Must set valid certificate for HTTPS"
            assert os.path.exists(self.cert), "Certificate file {} does not exist".format(self.cert)

        self.headers = {}

    def __enter__(self):
        self.client = tamrapi.Client(protocol=self.protocol,
                                     host=self.host,
                                     port=self.port,
                                     api_url=self.api_url,
                                     credentials=(self.user,self.pw))
        return self

    def __exit__(self, *args):
        self.client = None

    @check_status(success_code=200)
    def health(self):
        return self.client.get('/service/health')

    @check_status(success_code=200, silent=True)
    def get_datasets(self):
        """
        Get information about all datasets
        :return: requests.Response object
        """
        return self.client.get('/dataset/datasets')

    @check_status(success_code=200, silent=True)
    def get_dataset(self, name):
        ds_id = self.dataset_id(name)
        return self.client.get('/dataset/datasets/{}'.format(ds_id))

    @check_status(success_code=200, silent=True)
    def get_dataset_by_id(self, ds_id):
        return self.client.get('/dataset/datasets/{}'.format(ds_id))

    @check_status(success_code=201, silent=True)
    def create_project(self, project_name, project_type):
        return self.client.post('/recipe/projects/new/{0}'.format(project_type),
                                params={"name": project_name})

    def get_dataset_version(self, name):
        return int(self.get_dataset(name).json()['data']['version'])

    def is_dataset_materialized(self, name):
        revision = self.get_export_revision(name)
        version = self.get_dataset_version(name)
        if revision is None:
            return False
        return revision == version

    @check_status(success_code=200)
    def truncate_dataset(self, dataset_name):
        """
        Truncate a dataset. This will remove all records in the dataset, but not the dataset itself.
        :param dataset_name: Name of dataset
        :return: requests.Response object
        """
        print('Truncating Dataset')
        response = self.client.post('/dataset/datasets/{0}/truncate'.format(dataset_name),
                                    headers={'Content-Type': 'application/json',
                                             'Accept': 'application/json'})
        return response

    # TODO: works on command line but not with python subprocess
    def upload_dataset(self, id_field, description, file_path, generate_primary_key):
        """
        Create dataset and upload records.
        :param id_field:
        :param description:
        :param file_path:
        :param generate_primary_key:
        :return:
        """

        if not id_field:
            id_field = "primaryKey"

        fields = csv.DictReader(open(file_path, 'r'), delimiter=',').fieldnames
        curl_args = ['curl',
                     '-X',
                     'POST',
                     '-H',  '"Content-Type: multipart/form-data"',
                     '-H',  '"Accept: application/json"',
                     '-H',  '"Authorization: BasicCreds YWRtaW46ZHQ="',  # hardcoded, uses (admin, dt) TODO: make this better
                     '{protocol}://{host}:{port}{api_url}/procurement/datasets'.format(protocol=self.protocol,
                                                                                       host=self.host,
                                                                                       port=self.port,
                                                                                       api_url=self.api_url),
                     '-F', '"idField={id_field}"'.format(id_field=id_field),
                     '-F', '"description={description}"'.format(description=description),
                     '-F', '"generatePrimaryKey={generate_primary_key}"'.format(generate_primary_key=generate_primary_key),
                     '-F', 'file=@{file_path}'.format(file_path=file_path)]
        for field in fields:
            curl_args += ['-F', '"fields={field}"'.format(field=field)]
        print(' '.join([arg for arg in curl_args]))
        return subprocess.check_call(curl_args)

    def create_callback(self, encoder):
        encoder_len = encoder.len
        bar = ProgressBar(expected_size=encoder_len, filled_char='=')

        def callback(monitor):
            bar.show(monitor.bytes_read)

        return callback

    @check_status(success_code=201)
    def create_dataset(self, dataset_name, file_path, id_field, delimiter=',', encoding='utf-8', description=""):

        with open(file_path, encoding=encoding) as f:
            columns = list(csv.DictReader(f, delimiter=delimiter).fieldnames)

        if id_field:
            generate_primary_key = 'false'
        else:
            id_field = 'primaryKey'
            generate_primary_key = 'true'
        f = open(file_path)
        response = self.client.post('/procurement/datasets',
                                    files={'file': (dataset_name, f, 'text/plain')},
                                    data={'fields': columns, 'idField': id_field,
                                          'generatePrimaryKey': generate_primary_key,
                                          'description': description})
        f.close()
        return response

    def get_data(self, file_path):
        with open(file_path, 'rb') as f:
            for index, row in enumerate(f):
                if index % 100000 == 0:
                    print(index)
                yield row

    def write_json_create_file(self, in_filepath, id_field, out_filepath, encoding='utf-8', delimiter=','):
        print('Writing JSON CREATE File.')
        with open(in_filepath, 'r', encoding=encoding) as infile:
            with open(out_filepath, 'w') as outfile:
                csv_reader = csv.DictReader(infile, delimiter=delimiter)
                if id_field:
                    for row in csv_reader:
                        entry = {'action': 'CREATE',
                                 'recordId': row[id_field],
                                 'record': row}
                        outfile.write(json.dumps(entry))
                        outfile.write('\n')
                else:
                    for index, row in enumerate(csv_reader):
                        entry = {'action': 'CREATE',
                                 'recordId': index+1,
                                 'record': row}
                        outfile.write(json.dumps(entry))
                        outfile.write('\n')

    def upload_records(self, dataset_name, json_file_path, encoding='utf-8'):

        print('Uploading Records')
        response = self.client.post(endpoint='/dataset/datasets/{0}/update'.format(dataset_name),
                                    headers={'Content-Type': 'application/json'},
                                    data=self.get_data(json_file_path), stream=True)
        return response

    @check_status(success_code=200)
    def update_dataset(self, dataset_name, file_path, id_field, delimiter=',', encoding='utf-8', create_json=True):
        """
        Uploads content of records from csv. Assumes dataset has already been
        created (see: create method). Can be used for initial loading
        of records or to update the dataset records.  If updating, it will
        overwrite or create new records where necessary.
        :param dataset_name:
        :param file_path:
        :param id_field:
        :param delimiter:
        :param encoding:
        :param create_json:
        :return:
        """

        print('Updating Dataset')
        json_file_path = file_path + '.json'

        # Create JSON CREATE file
        if create_json:
            self.write_json_create_file(file_path, id_field, json_file_path, encoding=encoding, delimiter=delimiter)

        # Upload records
        response = self.upload_records(dataset_name, json_file_path, encoding=encoding)
        return response

    def dataset_exists(self, name):
        """
        Check to see if a dataset has already been created based on name.
        :param name:
        :return:
        """
        response = self.get_datasets()
        existing_datasets = [entry['data']['name'] for entry in response.json()]
        return name in existing_datasets

    @check_status(success_code=200)
    def initialize_unified_dataset(self, project_id, unified_dataset_name):
        response = self.client.post('/recipe/projects/init/{}'.format(project_id),
                                    json={"unifiedDatasetName": unified_dataset_name
                                          }
                                    )
        return response

    @check_status(success_code=200)
    def get_cluster_ids(self, unified_dataset_name, query='', offset='', limit='', sort='', cluster_ids='', name=''):
        print('Getting cluster IDs from Elastic.')
        response = self.client.get('/dedup/clusters/{}'.format(unified_dataset_name),
                                   params={'q':query,
                                           'offset':offset,
                                           'limit':limit,
                                           'sort':sort,
                                           'clusterIds':cluster_ids,
                                           'name':name
                                          }
                                  )
        return response

    @check_status(success_code=200)
    def split_cluster(self, unified_dataset_name, cluster_name_field, json_file_path):
        print('Splitting cluster.')
        with open(json_file_path, 'r') as json_file: 
            for row in json_file: 
                response = self.client.post('/dedup/clusters/{}/move-to-new'.format(unified_dataset_name),
                                            headers={'Content-Type': 'application/json'},
                                            params={"clusterNameField": cluster_name_field},
                                            data=row, 
                                            stream=True
                                           )
                                   
        return response
    
    @check_status(success_code=200)
    def assign_clusters(self, unified_dataset_name, usernames, cluster_ids):
        print('Assigning clusters.')
        response = self.client.post('/dedup/clusters/{}/feedback'.format(unified_dataset_name),
                                    json={"usernames": usernames,
                                          "clusterIds": cluster_ids
                                          }, 
                                    stream=True
                                    )
        return response

    @check_status(success_code=200)
    def unassign_clusters(self, unified_dataset_name, usernames, cluster_ids):
        print('Unassigning clusters.')
        response = self.client.delete('/dedup/clusters/{}/feedback'.format(unified_dataset_name),
                                      json={"usernames": usernames,
                                            "clusterIds": cluster_ids
                                            }
                                      )
        return response

    @check_status(success_code=200)
    def merge_clusters(self, unified_dataset_name, cluster_name_field, target_cluster_id, cluster_ids_to_merge):
        print('Merging clusters.')
        response = self.client.post('/dedup/clusters/{}/merge'.format(unified_dataset_name),
                                    params={'clusterNameField': cluster_name_field},
                                    json={'targetCluster': target_cluster_id,
                                          'clustersToMerge': cluster_ids_to_merge
                                          }, 
                                    stream=True
                                    )
        return response
            
    @check_status(success_code=200)
    def get_records_in_cluster(self, unified_dataset_name, cluster_id, sort_by_spend='false', limit=100):
        print('Fetching records in cluster.')
        response = self.client.get('/transactions/{}'.format(unified_dataset_name),
                                    params={'sortBySpend': sort_by_spend, 
                                           'suppliers': cluster_id, 
                                           'limit': limit}, 
                                    stream=True
                                    )
        return response

    @check_status(success_code=200)
    def get_assigned_clusters(self):
        print('Getting assigned clusters.')
        response = self.client.get('/dedup/clusters/feedback', 
                                  headers={'Content-Type': 'application/json'}, 
                                  stream=True)
        return response
    
    @check_status(success_code=202)
    def import_clusters(self, unified_dataset_name, cluster_name_field, recipe_id,
                        json_filepath):
        print('Importing clusters.')
        if isinstance(recipe_id, tuple):
            recipe_id = recipe_id[0]
        tag = 'recipe_{}'.format(recipe_id)

        with open(json_filepath) as json_file:
            response = self.client.post('/dedup/clusters/{0}/import?clusterNameField={1}&tag={2}'.format(
                unified_dataset_name,
                cluster_name_field,
                tag),
                headers={'Content-Type': 'application/json'},
                data=json_file,
                stream=True)
        return response

    @check_status(success_code=200)
    def get_users(self):
        return self.client.get(endpoint='/users',
                               headers={'Accept': 'application/json'})

    @check_status(success_code=204)
    def export_clusters(self, tag):
        return self.client.get('/dedup/clusters/export?tag={}'.format(tag))

    def dataset_id(self, name):
        """
        Returns dataset ID number for a dataset that has been created.
        :param name:
        :return:
        """
        response = self.get_datasets()
        ids = [entry['documentId']['id'] for entry in response.json()
               if entry['data']['name'] == name]

        if len(ids) == 0:
            return None

        return ids.pop()

    @check_status(success_code=200, silent=True)
    def get_taxonomies(self):
        return self.client.get('/taxonomy/taxonomies')

    def get_taxonomy_id(self, name):
        response = self.get_taxonomies()
        ids = [entry['documentId']['id'] for entry in response.json()
               if entry['data']['name'] == name]
        if len(ids) == 0:
            return None
        return ids.pop()

    @check_status(success_code=202)
    def upload_mastering_labels(self, labels, unified_dataset_name):
        response = self.client.put('/dedup/pairs/labels/{}'.format(unified_dataset_name),
                                   params={"includeUserResponse":"true"},
                                   data='\n'.join(json.dumps(l) for l in labels))
        return(response)

    @check_status(success_code=200)
    def post_categorization_labels(self, name,body, taxonomy, project_name):
        '''Takes a dataset name to label and the json body
        for labelling and posts them. Assumes a properly formatted body'''
        ds_id = self.dataset_id(name)
        tag = "recipe_{}".format(self.get_recipe_id(project_name, type='CATEGORIZATION'))
        response = self.client.post(
            '/procurement/datasets/{0}/categorizations?taxonomyName={1}&tag={2}'. \
                format(ds_id, taxonomy, tag),
            headers = { 'Content-Type': 'application/json',
                        'Accept': 'application/json'},
            data=json.dumps(body))
        return response

    @check_status(success_code=200)
    def post_transactions_categorize(self, df,
                                     dataset_name,
                                     project_name,
                                     transaction_id_field,
                                     category_id_field):
        #[{"datasetId": 76, "recordId": "-2814922735949283365",
        #  "categoryId": 159443, "tag": "recipe_23"}]

        #TODO: Think about how to improve this. should it be by record instead
        # of pssing the dataset_name and project name for all of them?
        dataset_id = self.dataset_id(dataset_name)
        tag = "recipe_{}".format(self.get_recipe_id(project_name, type='CATEGORIZATION'))
        body = []
        for i, row in df.iterrows():
            record = {"datasetId": dataset_id, "recordId": row[transaction_id_field],
                      "categoryId": row[category_id_field], "tag": tag}
            body.append(record)
        response = self.client.post('/transactions/categorize',
                                    headers = { 'Content-Type': 'application/json',
                                                'Accept': 'application/json'},
                                    data=json.dumps(body))
        return response


    @check_status(success_code=200)
    def delete_mastering_labels(self, unified_dataset_name):
        response = self.client.post('/dedup/pairs/labels/reset', params={"datasetName":unified_dataset_name},
                                    headers = { 'Content-Type': 'application/json',
                                                'Accept': 'application/json'})
        return(response)

    @check_status(success_code=200)
    def delete_categorization_labels(self, dataset_name, project_name, labels):
        '''Takes a dataset name to label and the json body
        for labelling and posts them. Assumes a properly formatted body'''
        ds_id = self.dataset_id(dataset_name)
        tag = "recipe_{}".format(self.get_recipe_id(project_name, type='CATEGORIZATION'))
        body = [{"datasetId": ds_id, "recordId": record_id} for record_id in labels]
        response = self.client.delete('/transactions/categorize?tag={}'.format(tag),
                                      headers = { 'Content-Type': 'application/json',
                                                  'Accept': 'application/json'},
                                      data=json.dumps(body))

        return response

    @check_status(success_code=200)
    def get_categorization_labels(self, ds_name, taxonomy_name, project_name, type='MANUAL'):
        ds_id = self.dataset_id(ds_name)
        tag = "recipe_{}".format(self.get_recipe_id(project_name, type='CATEGORIZATION'))
        taxonomy_id = self.get_taxonomy_id(taxonomy_name)
        response = self.client.get('/procurement/datasets/{0}/categorizations?taxonomyId={1}&tag={2}&type={3}'. \
                                   format(ds_id, taxonomy_id, tag, type),
                                   headers = {'Content-Type': 'application/json'})
        return response



    @check_status(success_code=200)
    def materialize_dataset_export(self, name):
        dataset_id = self.dataset_id(name)
        metadata = self.get_dataset(name).json()
        version = metadata['data']['version']
        columns = metadata['data']['fields']
        export_spec = {}
        export_spec['formatConfiguration'] = {
            "@class": "com.tamr.procurify.models.export.CsvFormat$Configuration",
            "delimiterCharacter": ",",
            "quoteCharacter": "\"",
            "nullValue": "",
            "writeHeader": "true"

        }
        export_spec['datasetId'] = dataset_id
        export_spec['revision'] = version
        export_spec['columns'] = columns
        return self.client.post(endpoint='/export',
                                headers={'Content-Type': 'application/json',
                                         'Accept': 'application/json'},
                                data=json.dumps(export_spec))
    @check_status(success_code=200)
    def read_dataset_export(self, name, num_tries=20):
        dataset_id = self.dataset_id(name)
        for i in range(num_tries):
            try:
                return self.client.get(endpoint='/export/read/dataset/{0}'.format(dataset_id))
            except (http.client.IncompleteRead,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError) as e:
                logging.warning("Error reading datasets: {}".format(e))
                logging.warning("Retrying.... Attempt {} out of {}".format(i+1, num_tries))
                time.sleep(5)

        raise Exception("Reading dataset failed after {} tries".format(num_tries))

    @check_status(success_code=200)
    def post_export_query(self, name):
        return self.client.post(endpoint='/export/query',
                                headers={'Content-Type': 'application/json',
                                         'Accept': 'application/json'},
                                data=json.dumps([name]))
    def get_export_revision(self, name):
        response = self.post_export_query(name).json()
        if len(response)==0:
            # No export has been generated before
            return None

        assert len(response)==1, "Expected only one dataset as a response, got {}!".format(len(response))
        return int(response[0]['data']['revision'])



    @check_status(success_code=200, silent=True)
    def get_job(self, job_id):
        return self.client.get(endpoint='/job/jobs/{0}'.format(job_id),
                               headers={'Accept': 'application/json'})

    @check_status(success_code=200, silent=True)
    def get_jobs(self):
        try:
            return self.client.get(endpoint='/job/jobs',
                                   headers={'Accept': 'application/json'},
                                   stream=True)
        except Exception as e:
            logging.error(e)
            raise

    def get_latest_job_id(self, description=None):
        response = self.get_jobs()
        # find job containing description text
        jobs = response.json()
        job_id = None
        for job in jobs:
            if description is not None:
                if job['data']['description'].find(description) > -1:
                    job_id = job['documentId']['id']
                    break
            else:
                job_id = job['documentId']['id']
                break
        if job_id == None:
            logging.warning('Job ID not found, check your dataset name')
        return job_id

    def wait_for_job(self, job_id,
                     poll_interval_seconds=5,
                     timeout_seconds=3600,
                     post_spark_wait_seconds=10):
        started = time.time()
        succeeded = False
        logging.info("Waiting for job...")
        while time.time() - started < timeout_seconds:
            response = self.get_job(job_id)
            status = response.json()['data']['status']['state']

            if status == 'SUCCEEDED':
                time.sleep(post_spark_wait_seconds)
                return True, response
            elif status == 'FAILED':
                return False, response
            elif status == 'PENDING':
                time.sleep(poll_interval_seconds)
            elif status == 'RUNNING':
                time.sleep(poll_interval_seconds)
            else:
                message = 'Unhandled status: {}'.format(status)
                logging.error(message)
                raise ValueError(message)

        message = 'Job {0} did not ' \
                  'finish within {1} seconds'.format(job_id,
                                                     timeout_seconds)
        logging.error(message)
        raise StandardError(message)

    @check_status(success_code=200)
    def get_pairs_export(self, tag):
        """
        Retrieve all pairs from mastering project
        :param tag: the dedup recipe id for your project, e.g. recipe_3
        :return: requests.Response with stream=true for fetching pairs.
        """
        return self.client.get('/dedup/pairs/export',
                               params={'tag':'recipe_{}'.format(tag)},
                               headers={'content-type': 'application/json'},
                               stream=True)

    @check_status(success_code=200)
    def get_pair_labels(self, unified_dataset_name, offset, manual_label, limit=5000):
        """
        Retrieve pairs from mastering project that matches specified manual_label.
        :param unified_dataset_name: unified dataset name of the mastering project
        :param limit: number of pairs to return, upper bound is 5000
        :param offset: number of pairs to skip
        :param manual_label: options include MISSING, MATCH, NON_MATCH
        :return: requests.Response object, pairs are stored in requests.Response.json()['items'].
        """
        response = self.client.get('/dedup/pairs/{}'.format(unified_dataset_name),
                                   params={"manualLabel":manual_label,
                                           "limit":limit,
                                           "offset":offset})
        return response

    @check_status(success_code=200)
    def get_clusters_export(self, tag):
        """
        Retrieve a stream of all clusters.
        :param tag: the schema mapping recipe id for your project e.g. recipe_2
        :return: requests.Response with stream=True for fetching clusters.
        """
        return self.client.get('/dedup/clusters/export',
                               params={'tag': tag},
                               headers={'content-type': 'application/json'},
                               stream=True
                               )

    @check_status(success_code=200, silent=True)
    def get_projects(self):
        return self.client.get('/recipe/projects/all',
                               headers=self.headers )

    @check_status(success_code=200, silent=True)
    def get_project(self, proj_id):
        return self.client.get('/recipe/projects/{}'.format(proj_id),
                               headers={'Content-Type': 'application/json',
                                        'Accept': 'application/json'})

    def get_project_id(self, name, description=None):
        response = self.get_projects()
        blob = response.json()
        for doc in blob:
            if doc['data']['name'] != name:
                continue
            if description is not None and \
                            doc['data']['description'] != description:
                continue
            return doc['documentId']['id']
        return None

    def get_clusters(self, tag):
        with generate_clusters(self.get_clusters_export(tag)) as clusters:
            # Clusters have the structure
            # {
            #     u'recordId': u'1234567',
            #     u'datasetName': u'dataset.csv',
            #     u'clusterId': u'12345678-9abc-def1-2345-6789abcdef01'
            # }
            # Create a map from recordId -> clusterId
            # If we were dealing with more than one dataset, we would want it to be
            # a map from (datasetName, recordId) -> clusterId
            cluster_dict =  {
                cluster['recordId']: cluster['clusterId']
                for cluster in clusters
            }
        return cluster_dict

    @check_status(success_code=202)
    def recipes_populate(self, recipe_id):
        #note, this should be the recipe id for the pairs/clusters (i.e. 3 for a lone project)
        if isinstance(recipe_id, tuple):
            recipe_id = recipe_id[0]

        return self.client.post(
            endpoint='/recipe/recipes/{0}/populate'.format(str(recipe_id)),
            headers={'Accept': 'application/json'})

    @check_status(success_code=201)
    def commit_schema(self, recipe_id):
        if isinstance(recipe_id, tuple):
            recipe_id = recipe_id[0]
        return self.client.post(
            endpoint='/recipe/recipes/{0}/run/records'.format(recipe_id),
            headers={'Accept': 'application/json'})

    @check_status(success_code=200, silent=True)
    def get_recipes(self):
        return self.client.get(endpoint='/recipe/recipes/all')

    def get_recipe_id(self, project_name, type='DEDUP'):
        """

        :param project_name: name of project to get recipe id for
        :param type: options are 'DEDUP', 'SCHEMA_MAPPING' or 'CATEGORIZATION'
        :return: returns the id as a number
        """
        assert project_name is not None, "Project name not specified."

        response = self.get_recipes()

        for document in response.json():
            if document['data']['project'] != project_name:
                continue
            if document['data']['type'] != type:
                continue
            recipe_id = document['documentId']['id']
            return int(recipe_id)
        logging.error("Couldn't find recipe id for project =",
                      project_name, "and type =", type)
        sys.exit(1)

    @check_status(success_code=200, silent=True)
    def get_recipe_by_id(self, recipe_id):
        return self.client.get(endpoint='/recipe/recipes/{}'.format(recipe_id))

    @check_status(success_code=200)
    def post_groups(self, group_name, roles, source_group_names, description=""):
        # This will only work if the client is configured to use the auth service!
        assert self.port == 9020, "Client must use auth service"
        assert isinstance(roles, list), \
            "Roles must be a list"
        assert isinstance(source_group_names, list), \
            "source_group_names must be a list"

        body = {}
        body['groupname'] = group_name
        body['description'] = description
        body['roles'] = roles
        body['sourceGroupNames'] = source_group_names

        return self.client.post(endpoint='/groups',
                                headers={'Content-Type': 'application/json',
                                         'Accept': 'application/json'},
                                data=json.dumps(body))

    @check_status(success_code=200)
    def get_user_preferences(self, user):
        # This will only work if the client is configured to use the auth service!
        user = user.replace("@", "%40")
        assert self.port == 9020, "Client must use auth service"
        return self.client.get(
            endpoint='/user-preferences/named/{}'.format(user),
            headers={'Content-Type': 'application/json',
                     'Accept': 'application/json'} )


    @check_status(success_code=200)
    def get_user_responses(self, user):
        """Returns json of user response information.
        Must used credentials of the user of interest"""
        data = {'filters': [
            {
                'path': {
                    'fields': [
                        'DATA',
                        'username'
                    ]
                },
                'type': 'EQUALS',
                'test': user
            }
        ]
        }
        data = json.dumps(data, indent=2)

        response = self.client.post(endpoint='/persistence/ns/feedback/query',
                                    headers={'Content-Type': 'application/json',
                                             'Accept': 'application/json'} ,
                                    data=data)
        return response

    @check_status(success_code=200)
    def set_user_preferences(self, user, preferences):
        # This will only work if the client is configured to use the auth service!
        assert self.port == 9020, "Client must use auth service"
        body = {
            "username": user,
            "preferences": preferences
        }
        return self.client.post(endpoint='/user-preferences',
                                headers={'Content-Type': 'application/json',
                                         'Accept': 'application/json'} ,
                                data=json.dumps(body))

    @check_status(success_code=200)
    def get_groups(self):
        return self.client.get(endpoint='/groups',
                               headers={'Accept': 'application/json'} )


def generate_clusters(response):
    """
    Given a Requests.Response prepared to stream clusters, return a generator that yields clusters.

    The returned generator must be closed, or it will leak a connection to the HTTP server.
    Recommended use is
    ```
    with dedup_client.generate_clusters(response) as clusters:
      for cluster in clusters:
        do_something_with(cluster)
    ```


    :param response: Requests.Response, as from get_clusters_export()
    :return: Generator that yields clusters.
    """
    class ClusterGenerator:
        def __init__(self, response):
            self.response = response
            self.lines = response.iter_lines()

        def __iter__(self):
            for line in self.lines:
                yield json.loads(line.decode('utf-8'))

        def close(self):
            self.response.close()

    # We want the close to be separate from iteration so clients will properly close
    # even if they don't consume the entire response.
    return contextlib.closing(
        ClusterGenerator(response)
    )
