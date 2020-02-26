import unittest
from lib import configloader


class TestConfigloader(unittest.TestCase):

    def setUp(self):
        pass

    # Returns True or False.  
    def test_getKey(self):
        parser = configloader.ConfigParser()
        parser.read_ini_file()
        self.assertEqual(parser.getKey('DEFAULT','ip'), '192.168.56.101')
      # Returns True or False. 
 
    
if __name__ == '__main__': 
    unittest.main() 