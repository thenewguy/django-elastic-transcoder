from boto import elastictranscoder
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from uuid import uuid4

class Command(BaseCommand):
    help = 'Updates the configuration of an elastic transcoder pipeline.  If the pipeline does not exist, it will be created.'
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--pipeline',
            dest='pipeline',
            help='The name of the pipeline to use.  Will be generated if not provided.  Pipeline will be created if it does not exist.',
        ),
        make_option(
            '--region',
            dest='region',
            help='Run python manage.py list_elastic_transcoder_regions for valid choices',
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
        valid_regions = [r.name for r in elastictranscoder.regions()]
        
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
        
        pipeline = kwargs["pipeline"] or "%s" % uuid4()
        
        region = kwargs["region"]
        if region and region not in valid_regions:
            raise CommandError('Invalid region specified.  Run python manage.py list_elastic_transcoder_regions for valid choices.')
        
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
                if region not in valid_regions:
                    raise CommandError('Invalid region specified as AWS_REGION on the settings module.  Run python manage.py list_elastic_transcoder_regions for valid choices.')
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
        #    retrieve elastic transcoder pipelines
        #
        self.stdout.write('Retrieving all elastic transcoder pipelines')
        
        details = None
        result = None
        token = None
        while result is None or token:
            result = connection.list_pipelines(page_token=token)
            token = result.get("NextPageToken", None)
            for p in result.get("Pipelines", []):
                if p["Name"] == pipeline:
                    details = p
                    token = None
                    break
        
        #
        #    create pipeline if it does not exist, otherwise update
        #
        bucket_config = {
            "Bucket": outputbucket,
            "Permissions": None,
            "GranteeType": None,
            "Grantee": None,
            "Access": None,
            "StorageClass": "ReducedRedundancy",
        }
        kwargs = {
            "name": pipeline,
            "input_bucket": inputbucket,
            "role": role,
            "notifications": {
                "Progressing": topicarn,
                "Completed": topicarn,
                "Warning": topicarn,
                "Error": topicarn,
            },
            "content_config": bucket_config,
            "thumbnail_config": bucket_config,
        }
        if details is None:
            self.stdout.write('Pipeline "%s" did not exist.' % pipeline)
            response = connection.create_pipeline(**kwargs)
            self.stdout.write('Created pipeline with id "%s".' % response["Pipeline"]["Id"])
        else:
            self.stdout.write('Pipeline "%s" already exists, updating now.' % pipeline)
            response = connection.create_pipeline(**kwargs)
            self.stdout.write('Updated pipeline with id "%s".' % details["Id"])