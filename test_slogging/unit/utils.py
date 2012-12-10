import unittest


class Mocker(object):
    def __init__(self):
        self._dict = {}

    def __call__(self, obj, name, value):
        try:
            self._dict[(obj, name)] = getattr(obj, name)
        except AttributeError:
            self._dict[(obj, name)] = None
        setattr(obj, name, value)

    def restore(self):
        for key, value in self._dict.iteritems():
            if value is None:
                delattr(key[0], key[1])
            else:
                setattr(key[0], key[1], value)
        self._dict = {}


class MockerTestCase(unittest.TestCase):
    def setUp(self):
        self.mock = Mocker()

    def tearDown(self):
        self.mock.restore()
