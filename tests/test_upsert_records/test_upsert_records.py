import os
import unittest
import modules.upsert_records as mod
import global_config


class TestModule(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.getcwd()

    def test_is_valid_file(self):
        valid_filepath = os.path.join(self.test_dir, 'testfile.csv')
        invalid_filepath = os.path.join(self.test_dir, 'testfile.txt')
        self.assertTrue(mod.is_valid_file(valid_filepath))
        self.assertRaises(FileNotFoundError, mod.is_valid_file, invalid_filepath)

    def test_update_dataset(self):
        dataset_name = 'testfile_with_primary_key.csv'
        csv_filepath = os.path.join(self.test_dir, 'testfile.csv')
        id_field = 'name'

        response = mod.update_dataset(host=global_config.host,
                                    protocol=global_config.protocol,
                                    port=global_config.port,
                                    username=global_config.username,
                                    password=global_config.password,
                                    dataset_name=dataset_name,
                                    csv_filepath=csv_filepath,
                                    id_field=id_field)

        self.assertTrue(response.json()['successful'])


if __name__ == "__main__":
    unittest.main()
