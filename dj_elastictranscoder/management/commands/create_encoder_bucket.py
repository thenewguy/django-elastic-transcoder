from boto.exception import BotoServerError
from boto.s3.connection import Location, S3Connection
from django.core.management.base import BaseCommand, CommandError
from json import dumps
from optparse import make_option
from uuid import uuid4

REGION_MAP = dict([(getattr(Location, i) or "us-east-1", getattr(Location, i)) for i in dir(Location) if i[0].isupper()])
VALID_REGIONS = set(REGION_MAP.keys())

class Command(BaseCommand):
    help = 'Creates an S3 bucket'
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--region',
            dest='region',
            help='One of {0}'.format(VALID_REGIONS),
        ),
        make_option(
            '--bucket',
            dest='bucket',
            help='The name of the bucket to create.  Will be generated if not provided.',
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
        
        bucket = kwargs["bucket"] or "elastic-transcoder-{0}".format(uuid4())
        
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
        
        conn = S3Connection(access_key_id, secret_access_key)
        conn.create_bucket(bucket, location=REGION_MAP[region])
        
        log('Created bucket %s' % bucket)
        
        if kwargs["json"]:
            return dumps({"bucket": bucket})