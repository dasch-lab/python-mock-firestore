from typing import Iterable, Sequence, Optional
from mockfirestore.collection import CollectionReference, CollectionGroupReference
from mockfirestore.document import DocumentReference, DocumentSnapshot
from mockfirestore.query import CollectionGroup
from mockfirestore.transaction import Transaction
from mockfirestore.transaction import BatchTransaction

class MockFirestore:

    def __init__(self) -> None:
        self._data = {}

    def _ensure_path(self, path):
        current_position = self

        for el in path[:-1]:
            if type(current_position) in (MockFirestore, DocumentReference):
                current_position = current_position.collection(el)
            else:
                current_position = current_position.document(el)

        return current_position

    def document(self, path: str) -> DocumentReference:
        path = path.split("/")

        if len(path) % 2 != 0:
            raise Exception("Cannot create document at path {}".format(path))
        current_position = self._ensure_path(path)

        return current_position.document(path[-1])

    def collection(self, path: str) -> CollectionReference:
        path = path.split("/")

        if len(path) % 2 != 1:
            raise Exception("Cannot create collection at path {}".format(path))

        name = path[-1]
        if len(path) > 1:
            current_position = self._ensure_path(path)
            return current_position.collection(name)
        else:
            if name not in self._data:
                self._data[name] = {}
            return CollectionReference(self._data, [name])

    def collection_group(self, collection_id: str) -> CollectionGroup:
        if '/' in collection_id:
            raise ValueError(
                "Invalid collection_id "
                + collection_id
                + ". Collection IDs must not contain '/'."
            )

        _, keys = _get_collection_group_data(self._data, collection_id)

        return CollectionGroup(CollectionGroupReference(self._data, keys))

    def collections(self) -> Sequence[CollectionReference]:
        return [CollectionReference(self._data, [collection_name]) for collection_name in self._data]
    
    def batch(self) -> BatchTransaction:
        return BatchTransaction(self)

    def reset(self):
        self._data = {}

    def get_all(self, references: Iterable[DocumentReference],
                field_paths=None,
                transaction=None) -> Iterable[DocumentSnapshot]:
        for doc_ref in set(references):
            yield doc_ref.get()

    def transaction(self, **kwargs) -> Transaction:
        return Transaction(self, **kwargs)


def _get_collection_group_data(data: dict, name: str, output: Optional[dict] = None, depth=0, path=[]) -> dict:
    """
    Recursively get the data for a collection group.

    Args:
        data: The root data or document data to search.
        name: The name of the collection group.
        output: The flat output dictionary.

    Returns:
        A flat dictionary containing all the data for the collection group.
    """
    output = output or {}
    if depth % 2 == 0 and name in data:
        new_output = {}
        for k in data[name]:
            v = data[name][k]
            if len(path) == 0:
                new_output[k] = v
            else:
                new_output['/'.join(path) + '/' + name + '/' + k] = v
        output.update(new_output)
        return output, None
    else:
        for k in data:
            documents_in_collection = data[k]
            if isinstance(documents_in_collection, dict):
                if len(path) == 0:
                    new_parent_path = [k]
                else:
                    new_parent_path = path + [k]
                ret, _ = _get_collection_group_data(documents_in_collection, name, output, depth+1, new_parent_path)
                output.update(ret)
    
    if depth == 0:
        new_output = {}
        keys = output.keys()
        for k in output:
            sub_path = k.split('/')
            pointer = new_output
            for sub in sub_path[:-1]:
                if sub not in pointer:
                    pointer[sub] = {}
                pointer = pointer[sub]
            pointer[sub_path[-1]] = output[k]
        output = new_output

        new_keys = []
        for key in keys:
            new_keys.append(key.split('/'))
        return output, new_keys

    return output, None
