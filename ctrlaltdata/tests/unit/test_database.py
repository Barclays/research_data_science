import unittest
import io
import sys
import pyodbc

from ...database import SqlReader
from ...config import QAD_CONNECTION_STRING


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.obj = SqlReader()
    
    def test_connection_object(self):
        assert self.obj.connection is None
    
    def test_query(self):
        self.obj.connection = pyodbc.connect(QAD_CONNECTION_STRING)
        test_query = "SELECT  TOP 5 * FROM TreCode"
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput
        result = self.obj.query(test_query, verbose=True)
        assert not result.empty
        assert capturedOutput.getvalue().strip() == test_query.strip()
    
    def test_get_tables(self):
        self.obj.connection = pyodbc.connect(QAD_CONNECTION_STRING)
        result = self.obj.get_tables()
        assert not result.empty


if __name__ == '__main__':
    unittest.main()