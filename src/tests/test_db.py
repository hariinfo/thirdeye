import unittest
from lib import db

class TestDB(unittest.TestCase):

    def setUp(self):
        pass

    # Returns True or False.  
    def test_getConnection(self):
        conn = db.db()          
        self.assertTrue(conn.getConnection()) 
      # Returns True or False.  
    
    def test_getCloseConnection(self):
        conn = db.db()          
        self.assertTrue(conn) 

if __name__ == '__main__': 
    unittest.main() 