from boto import iam
from boto.exception import BotoServerError
from django.core.management.base import BaseCommand, CommandError
from json import dumps
from optparse import make_option
from uuid import uuid4

VALID_REGIONS = set([r.name for r in iam.regions()])

class Command(BaseCommand):
    help = 'Creates an IAM role for the Elastic Transcoder Service to use'
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--region',
            dest='region',
            help='One of {0}'.format(VALID_REGIONS),
        ),
        make_option(
            '--role',
            dest='role',
            help='The name of the role to create.  Will be generated if not provided.',
        ),
        make_option(
            '--json',
            dest='json',
            action='store_true',
            default=False,
            help='Only output return values as a json encoded string.  There is no status output.',
        ),
    )

    def handle(self, *args, **kwargs):
        #
        #    initial config
        #
        from django.conf import settings
        
        #
        #    parse inputs
        #
        region = kwargs["region"]
        if region and region not in VALID_REGIONS:
            raise CommandError('Invalid region specified.  Region must be one of {0}.'.format(VALID_REGIONS))
        
        role = kwargs["role"] or "elastic-transcoder-{0}".format(uuid4())
        
        if kwargs["json"]:
            log = lambda x: None
        else:
            log = lambda x: self.stdout.write(x)
        #
        #    reference settings
        #
        access_key_id = getattr(settings, 'AWS_ACCESS_KEY_ID', None)
        secret_access_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY', None)
        
        if access_key_id is None:
            raise CommandError('Please provide AWS_ACCESS_KEY_ID on the settings module')

        if secret_access_key is None:
            raise CommandError('Please provide AWS_SECRET_ACCESS_KEY on the settings module')
        
        if region is None:
            region = getattr(settings, 'AWS_REGION', None)
            
            if region:
                if region not in VALID_REGIONS:
                    raise CommandError('Invalid region specified as AWS_REGION on the settings module.  Region must be one of {0}.'.format(VALID_REGIONS))
            else:
                log('Region was not specified on the command line or on the settings module.  Proceeding without setting the S3 region.  This is not recommended.')
        
        # policy figured out by documentation plus trial and error.  appears
        # to produce the same output as the elastic transcoder web console
        assume_policy = {
            "Version":"2012-10-17",
            "Statement":[
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "elastictranscoder.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        # policy copied from the default set by the elastic transcoder web interface
        access_policy = '{"Version":"2008-10-17","Statement":[{"Sid":"1","Effect":"Allow","Action":["s3:ListBucket","s3:Put*","s3:Get*","s3:*MultipartUpload*"],"Resource":"*"},{"Sid":"2","Effect":"Allow","Action":"sns:Publish","Resource":"*"},{"Sid":"3","Effect":"Deny","Action":["s3:*Policy*","sns:*Permission*","sns:*Delete*","s3:*Delete*","sns:*Remove*"],"Resource":"*"}]}'
        
        connection = iam.connection.IAMConnection(access_key_id, secret_access_key)
        response = connection.create_role(role, assume_role_policy_document=assume_policy, path="/")
        connection.put_role_policy(role, "ets-console-generated-policy-copied-on-2014-05-18", access_policy)
        
        arn = response.get("create_role_response", {}).get("create_role_result", {}).get("role", {})["arn"]
        log('Created role "{0}" with ARN "{1}"'.format(role, arn))
        
        if kwargs["json"]:
            return dumps({"role": role, "arn": arn})