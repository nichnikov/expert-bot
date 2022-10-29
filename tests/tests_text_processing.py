import time
from unittest import TestCase
from src.texts_processing import TextsTokenizer


# TODO: add main() in unittests
class TextProssingTest(TestCase):
    """"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up for class"""
        print("setUpClass")
        print("==========")

    @classmethod
    def tearDownClass(cls):
        """Tear down for class"""
        print("==========")
        print("tearDownClass")

    def setUp(self):
        """Set up for test"""
        self.startTime = time.time()
        print("\nSet up for\n[" + self.shortDescription() + "]")

    def tearDown(self):
        # Tear down for test
        t = time.time() - self.startTime
        print('%s: %.3f' % (self.id(), t))
        print("\nTear down for\n[" + self.shortDescription() + "]")
        print("")

    def test_add_stopwords(self):
        """Checking for the correct addition of stop words to the TextsTokenizer"""
        tknz = TextsTokenizer()
        stopwords = ["a", "b"]
        texts = ["a b a s", "a c e f", "d a g"]
        tknz.add_stopwords(stopwords)
        test_result = tknz(texts)
        true_result = [['s'], ['c', 'e', 'f'], ['d', 'g']]
        self.assertEqual(test_result, true_result)

    """
    def test_texts2tokens(self):
        """"""
        pass

    def test_tokenization(self):
        """"""
        pass"""

