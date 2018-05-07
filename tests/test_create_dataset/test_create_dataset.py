import os
import unittest
import common.create_dataset as mod
from common.tests import tamr_config


class TestModule(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.getcwd()

    def test_is_valid_file(self):
        valid_filepath = os.path.join(self.test_dir, 'testfile.csv')
        invalid_filepath = os.path.join(self.test_dir, 'testfile.txt')
        self.assertTrue(mod.is_valid_file(valid_filepath))
        self.assertRaises(FileNotFoundError, mod.is_valid_file, invalid_filepath)

    def test_create_dataset_no_primary_key(self):
        dataset_name = 'testfile_no_primary_key.csv'
        csv_filepath = os.path.join(self.test_dir, 'testfile.csv')
        description = 'testfile_no_primary_key'
        id_field = None

        self.assertEqual(mod.create_dataset(host=tamr_config.HOST,
                                            protocol=tamr_config.PROTOCOL,
                                            port=tamr_config.PORT,
                                            username=tamr_config.USERNAME,
                                            password=tamr_config.PASSWORD,
                                            dataset_name=dataset_name,
                                            csv_filepath=csv_filepath,
                                            id_field=id_field,
                                            description=description).status_code, 201)

    def test_create_dataset_with_primary_key(self):
        dataset_name = 'testfile_with_primary_key.csv'
        csv_filepath = os.path.join(self.test_dir, 'testfile.csv')
        description = 'testfile_with_primary_key'
        id_field = 'name'

        self.assertEqual(mod.create_dataset(host=tamr_config.HOST,
                                            protocol=tamr_config.PROTOCOL,
                                            port=tamr_config.PORT,
                                            username=tamr_config.USERNAME,
                                            password=tamr_config.PASSWORD,
                                            dataset_name=dataset_name,
                                            csv_filepath=csv_filepath,
                                            id_field=id_field,
                                            description=description).status_code, 201)


if __name__ == "__main__":
    unittest.main()
