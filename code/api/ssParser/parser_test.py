import unittest
from test_utils import easy_test

# Sample integration tests for Shopping Stories Parser
# Put the input in a file named <yourtestcase>.xlsx in the inputs folder
# Put the expected output in a file named <yourtestcase>.json in the outputs folder
# Make a function in the ParserTestClass named <yourtestcase> (IMPORTANT: <yourtestcase> MUST START WITH test_ OR UNITTEST WILL NOT RUN IT)
# following the format of the existing test cases.

class ParserTest(unittest.TestCase):
    # Note that the data in the expected output is probably not correct, this is just a sample test case that fails.
    # Read the documentation for easy_test in test_utils.py if you wish to understand how it works
    @easy_test
    def test_cotton_handker(self, /, output, expected_output):
        # self.maxDiff = 90000
        # Uncomment that if you want diff for failures
        self.assertEqual(output, expected_output)


    # Note that the data in the expected output is not actually correct, this is just serving as an example of a test succceeding for now.
    @easy_test
    def test_pound_sugar(self, /, output, expected_output):
        self.assertEqual(output, expected_output)

if __name__ == "__main__":
    unittest.main()