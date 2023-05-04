from hamcrest import assert_that, equal_to

from nodestream.normalizers import Normalizer

def test_arugment_flag():
    class Test(Normalizer, name="test"):
        pass

    assert_that(Test.arugment_flag(), equal_to("do_test"))
