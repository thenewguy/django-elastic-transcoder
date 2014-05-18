from boto import elastictranscoder
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option

VALID_REGIONS = set([r.name for r in elastictranscoder.regions()])

class Command(BaseCommand):
    help = 'Tests the suitability of an IAM role for usage with elastic transcoder'
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--region',
            dest='region',
            help='One of {0}'.format(VALID_REGIONS),
        ),
        make_option(
            '--inputbucket',
            dest='inputbucket',
            help='The S3 bucket name that will be used to upload content to the pipeline',
        ),
        make_option(
            '--outputbucket',
            dest='outputbucket',
            help='The S3 bucket name that will be used to download content from the pipeline',
        ),
        make_option(
            '--topicarn',
            dest='topicarn',
            help='The ARN of the SNS topic that will be used for status updates from the pipeline',
        ),
        make_option(
            '--role',
            dest='role',
            help='The IAM Role that the pipeline will use to access resources',
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
        inputbucket = kwargs["inputbucket"]
        if inputbucket is None:
            inputbucket = getattr(settings, 'ELASTIC_TRANSCODER_INPUT_BUCKET', None)
            if inputbucket is None:
                raise CommandError("One of either the 'inputbucket' kwarg or 'ELASTIC_TRANSCODER_INPUT_BUCKET' setting is required.")
        
        outputbucket = kwargs["outputbucket"]
        if outputbucket is None:
            outputbucket = getattr(settings, 'ELASTIC_TRANSCODER_OUTPUT_BUCKET', None)
            if outputbucket is None:
                raise CommandError("One of either the 'outputbucket' kwarg or 'ELASTIC_TRANSCODER_OUTPUT_BUCKET' setting is required.")

        
        topicarn = kwargs["topicarn"]
        if topicarn is None:
            topicarn = getattr(settings, 'ELASTIC_TRANSCODER_TOPIC_ARN', None)
            if topicarn is None:
                raise CommandError("One of either the 'topicarn' kwarg or 'ELASTIC_TRANSCODER_TOPIC_ARN' setting is required.")
        
        role = kwargs["role"]
        if role is None:
            role = getattr(settings, 'ELASTIC_TRANSCODER_IAM_ROLE', None)
            if role is None:
                raise CommandError("One of either the 'role' kwarg or 'ELASTIC_TRANSCODER_IAM_ROLE' setting is required.")
        
        region = kwargs["region"]
        if region and region not in VALID_REGIONS:
            raise CommandError('Invalid region specified.  Region must be one of {0}.'.format(VALID_REGIONS))
        
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
                self.stdout.write('Region was not specified on the command line or on the settings module.  Proceeding without setting the elastic transcoder region.  This is not recommended.')
        
        if region:
            elastictranscoder.connect_to_region(region)
        
        #
        #    connect to elastic transcoder
        #
        self.stdout.write('Creating elastic transcoder connection')
        connection = elastictranscoder.layer1.ElasticTranscoderConnection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        
        #
        #    test iam role for usage with an elastic transcoder pipeline
        #
        self.stdout.write('Testing IAM role for usage with an elastic transcoder pipeline.')
        response = connection.test_role(role=role, input_bucket=inputbucket, output_bucket=outputbucket, topics=[topicarn])
        success = response["Success"].lower() == "true"
        messages = response["Messages"]
        
        if success:
            self.stdout.write('Test passed.')
        else:
            self.stdout.write('Test failed.')
        if messages:
            self.stdout.write('The following messages were provided:')
            for i, message in enumerate(messages):
                if i:
                    self.stdout.write('')
                self.stdout.write('{0})\n{1}'.format(i+1, message))