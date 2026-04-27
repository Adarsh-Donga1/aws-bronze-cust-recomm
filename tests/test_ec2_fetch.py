import unittest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from bronze.config.job_params import JobParams
from bronze.services.aws.ec2 import fetch_ec2_resources, reset_describe_soft_deny_stats


def _job_params() -> JobParams:
    return JobParams(
        job_name="t",
        window_days=7,
        active_services=["EC2"],
        account_id="123456789012",
        client_id="c1",
        additional_client_id="",
        aws_regions=["us-east-1"],
        iceberg_catalog="glue_catalog",
        iceberg_database="bronze",
        s3_bucket="test-bucket",
        aws_profile="",
        aws_credentials_secret="",
    )


class TestFetchEc2Resources(unittest.TestCase):
    def setUp(self) -> None:
        reset_describe_soft_deny_stats()

    def test_auth_failure_returns_empty(self):
        client = MagicMock()
        paginator = MagicMock()

        def _raise(_self=None, **_k):
            raise ClientError(
                {"Error": {"Code": "AuthFailure", "Message": "not authorized"}},
                "DescribeInstances",
            )

        paginator.paginate.side_effect = _raise
        client.get_paginator.return_value = paginator
        out = fetch_ec2_resources(client, _job_params(), "ap-east-1", "123456789012")
        self.assertEqual(out, [])

    def test_unexpected_client_error_reraises(self):
        client = MagicMock()
        paginator = MagicMock()

        def _raise(_self=None, **_k):
            raise ClientError(
                {"Error": {"Code": "InvalidRequest", "Message": "bad"}},
                "DescribeInstances",
            )

        paginator.paginate.side_effect = _raise
        client.get_paginator.return_value = paginator
        with self.assertRaises(ClientError):
            fetch_ec2_resources(client, _job_params(), "us-east-1", "123456789012")

    def test_one_page_returns_rows(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-0abc",
                                "State": {"Name": "running"},
                                "Tags": [{"Key": "Name", "Value": "web"}],
                            }
                        ]
                    }
                ]
            }
        ]
        client.get_paginator.return_value = paginator
        out = fetch_ec2_resources(client, _job_params(), "us-east-1", "123456789012")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["instance_id"], "i-0abc")
        self.assertIn("client_id", out[0])


if __name__ == "__main__":
    unittest.main()
