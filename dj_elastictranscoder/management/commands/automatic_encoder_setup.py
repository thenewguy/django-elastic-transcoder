from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from itertools import chain
from json import loads
from optparse import make_option
from StringIO import StringIO
from uuid import uuid4
from .create_encoder_bucket import VALID_REGIONS as VALID_REGIONS_S3
from .create_encoder_iam_role import VALID_REGIONS as VALID_REGIONS_IAM
from .create_encoder_topic import VALID_REGIONS as VALID_REGIONS_SNS
from .update_encoder_pipeline import VALID_REGIONS as VALID_REGIONS_ET

VALID_REGIONS = set(chain(VALID_REGIONS_S3, VALID_REGIONS_IAM, VALID_REGIONS_SNS, VALID_REGIONS_ET))

class Command(BaseCommand):
    help = 'Configures everything to get up and running with elastic transcoder.'

    option_list = BaseCommand.option_list + (
        make_option(
            '--region',
            dest='region',
            help='One of {0}'.format(VALID_REGIONS),
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
        
        #
        #    reference settings
        #
        if region is None:
            region = getattr(settings, 'AWS_REGION', None)
            
            if region:
                if region not in VALID_REGIONS:
                    raise CommandError('Invalid region specified as AWS_REGION on the settings module.  Region must be one of {0}.'.format(VALID_REGIONS))
            else:
                self.stdout.write('Region was not specified on the command line or on the settings module.  Proceeding without setting the region.  This is not recommended.')
        
        # create a StringIO instance to catch output from other commands
        fp = StringIO()
        
        self.stdout.write('Create the input bucket.')
        call_command("create_encoder_bucket", region=region, json=True, stdout=fp)
        in_bucket = loads(fp.getvalue())["bucket"]
        fp.truncate(0)
        
        self.stdout.write('Create the output bucket.')
        call_command("create_encoder_bucket", region=region, json=True, stdout=fp)
        out_bucket = loads(fp.getvalue())["bucket"]
        fp.truncate(0)
        
        self.stdout.write('Create the IAM role.')
        call_command("create_encoder_iam_role", region=region, json=True, stdout=fp)
        role_dict = loads(fp.getvalue())
        role = role_dict["role"]
        role_arn = role_dict["arn"] 
        fp.truncate(0)
        
        self.stdout.write('Create the SNS topic.')
        call_command("create_encoder_topic", region=region, json=True, stdout=fp)
        topic_dict = loads(fp.getvalue())
        topic = topic_dict["name"]
        topic_arn = topic_dict["arn"] 
        fp.truncate(0)
        
        self.stdout.write('Create the pipeline.')
        pipeline = "%s" % uuid4()
        call_command(
            "update_encoder_pipeline",
            region=region,
            inputbucket=in_bucket,
            outputbucket=out_bucket,
            topicarn=topic_arn,
            role=role_arn,
            pipeline=pipeline,
            stdout=fp,
        )
        
        self.stdout.write('~'*16)
        self.stdout.write('SUMMARY')
        self.stdout.write('~'*16)
        self.stdout.write('in_bucket: %s' % in_bucket)
        self.stdout.write('out_bucket: %s' % out_bucket)
        self.stdout.write('role: %s' % role)
        self.stdout.write('role_arn: %s' % role_arn)
        self.stdout.write('topic: %s' % topic)
        self.stdout.write('topic_arn: %s' % topic_arn)
        self.stdout.write('pipeline: %s' % pipeline)