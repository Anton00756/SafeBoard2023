import json
import time
from unittest import TestCase, main as test_main
from server import api, init_db, threading_queue
import tempfile
import os


class TestSearchAPI(TestCase):
    def setUp(self):
        self.db_fd, api.config['DATABASE'] = tempfile.mkstemp()
        api.testing = True
        self.app = api.test_client()
        self.default_folder = api.config['DATA_FOLDER']
        with api.app_context():
            init_db()

    def tearDown(self):
        threading_queue.join()
        os.close(self.db_fd)
        os.unlink(api.config['DATABASE'])
        api.config['DATA_FOLDER'] = self.default_folder

    def test_post(self):
        response = self.app.post('/search', json=json.dumps(dict(
            text='abc',
            file_mask="*a*.???",
            size=dict(
                value=42000,
                operator="gt"
            ),
            creation_time=dict(
                value="2020-03-03T14:00:54Z",
                operator="gt"
            )
        )))
        self.assertEqual(response.status, "200 OK")
        self.assertIn("search_id", json.loads(response.text))

    def test_incorrect_get_id(self):
        response = self.app.get(f'/searches/incorrect_search_id')
        self.assertEqual(response.status, "400 BAD REQUEST")

    def test_correct_get_full(self):
        response = self.app.post('/search', json=json.dumps(dict(
            text='abc',
            file_mask="*i*.???",
            size=dict(
                value=3,
                operator="ge"
            ),
            creation_time=dict(
                value="2020-03-03T14:00:54Z",
                operator="gt"
            )
        )))
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)

    def test_get_all_files(self):
        response = self.app.post('/search')
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)
        self.assertIsNotNone(result.get('paths'))
        self.assertEqual(len(result.get('paths')), 7)

    def test_get_with_changed_folder(self):
        api.config['DATA_FOLDER'] = "data_directory/folder1"
        response = self.app.post('/search')
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)
        self.assertIsNotNone(result.get('paths'))
        self.assertEqual(len(result.get('paths')), 2)

    def test_get_with_only_text_field(self):
        response = self.app.post('/search', json=json.dumps(dict(text='bc')))
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)
        self.assertEqual(result.get('paths'), ['data_directory\\folder1\\file2.txt',
                                               'data_directory\\folder2\\folder3\\file5.txt',
                                               'data_directory\\folder2\\folder3\\text_png.bmp',
                                               'data_directory\\zip_folder\\zip_data.zip\\file6.txt',
                                               'data_directory\\zip_folder\\zip_data.zip\\text_png.bmp'])

    def test_get_in_zip_folder(self):
        api.config['DATA_FOLDER'] = "data_directory/zip_folder"
        response = self.app.post('/search', json=json.dumps(dict(text='abc')))
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)
        self.assertEqual(result.get('paths'), ['data_directory\\zip_folder\\zip_data.zip\\file6.txt',
                                               'data_directory\\zip_folder\\zip_data.zip\\text_png.bmp'])

    def test_get_with_nonexistent_fields(self):
        response = self.app.post('/search', json=json.dumps(dict(search_text='abc', byte_size=dict(value=5))))
        time.sleep(0.2)
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), True)
        self.assertIsNotNone(result.get('paths'))
        self.assertEqual(len(result.get('paths')), 7)

    def test_not_finished_get(self):
        response = self.app.post('/search')
        response = self.app.get(f'/searches/{json.loads(response.text).get("search_id")}')
        self.assertEqual(response.status, "200 OK")
        result = json.loads(response.text)
        self.assertEqual(result.get('finished'), False)


if __name__ == "__main__":
    test_main(verbosity=2)
