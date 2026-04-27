from bronze.services.aws.ec2 import EC2_SERVICE  # noqa: F401
from bronze.services.registry import register

register(EC2_SERVICE)
