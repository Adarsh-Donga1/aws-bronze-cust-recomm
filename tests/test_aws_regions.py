import os
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from bronze.config.aws_regions import _fallback_ec2_regions_no_describe_permission, list_all_enabled_ec2_regions


class TestAwsRegionsFallback(unittest.TestCase):
    def tearDown(self):
        for k in (
            "EC2_BRONZE_REGION_FALLBACK",
            "EC2_BRONZE_USE_FULL_METADATA_REGIONS",
        ):
            os.environ.pop(k, None)

    def test_explicit_env_list(self):
        os.environ["EC2_BRONZE_REGION_FALLBACK"] = "us-west-2,eu-west-1"
        sess = MagicMock()
        r = _fallback_ec2_regions_no_describe_permission(sess)
        self.assertEqual(r, ["eu-west-1", "us-west-2"])

    @patch("bronze.config.aws_regions.STANDARD_COMMERCIAL_EC2_REGIONS", ("us-east-1", "us-west-2"))
    def test_describe_regions_denied_uses_standard(self):
        ec2 = MagicMock()
        ec2.describe_regions.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}},
            "DescribeRegions",
        )
        session = MagicMock()
        session.client.return_value = ec2
        regions = list_all_enabled_ec2_regions(session)
        self.assertIn("us-east-1", regions)
        self.assertIn("us-west-2", regions)
        self.assertEqual(len(regions), 2)


if __name__ == "__main__":
    unittest.main()
