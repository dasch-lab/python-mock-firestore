from unittest import TestCase
from google.cloud.firestore_v1.field_path import FieldPath
from mockfirestore import MockFirestore


class TestMockFirestore(TestCase):
    def test_client_get_all(self):
        fs = MockFirestore()
        fs._data = {'foo': {
            'first': {'id': 1},
            'second': {'id': 2}
        }}
        doc = fs.collection('foo').document('first')
        results = list(fs.get_all([doc]))
        returned_doc_snapshot = results[0].to_dict()
        expected_doc_snapshot = doc.get().to_dict()
        self.assertEqual(returned_doc_snapshot, expected_doc_snapshot)

    def test_client_collections(self):
        fs = MockFirestore()
        fs._data = {
            'foo': {
                'first': {'id': 1},
                'second': {'id': 2}
            },
            'bar': {}
        }
        collections = fs.collections()
        expected_collections = fs._data

        self.assertEqual(len(collections), len(expected_collections))
        for collection in collections:
            self.assertTrue(collection._path[0] in expected_collections)
    
    def test_sort_by_value(self):
        fs = MockFirestore()
        fs._data = {
            'foo': {
                'first': {'id': 1},
                'second': {'id': 2}
            }
        }

        collection = fs.collection('foo')
        docs = collection.order_by('id', direction='ASCENDING').stream()
        docs = [doc.to_dict() for doc in docs]
        self.assertEqual(docs, [{'id': 1}, {'id': 2}])
    
    def test_sort_by_key(self):
        fs = MockFirestore()
        fs.collection('foo').document('first').set({'id': 1})
        fs.collection('foo').document('second').set({'id': 2})

        collection = fs.collection('foo')
        docs = collection.order_by(FieldPath.document_id(), direction='ASCENDING').stream()
        docs = [doc.to_dict() for doc in docs]
        self.assertEqual(docs, [{'id': 1}, {'id': 2}])