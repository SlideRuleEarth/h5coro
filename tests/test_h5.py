"""Tests for h5 endpoint."""

import pytest
import h5coro


@pytest.mark.network
class TestApi:
    def test_happy_case(self, daac):
        assert h5coro.placeholder({"daac":daac}) == True

