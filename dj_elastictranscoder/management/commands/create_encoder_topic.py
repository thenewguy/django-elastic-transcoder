from boto import sns
from boto.exception import BotoServerError
from django.core.management.base import BaseCommand, CommandError
from json import dumps
from optparse import make_option
from uuid import uuid4

VALID_REGIONS = tuple([r.name for r in sns.regions()])

class Command(BaseCommand):
    help = 'If the topic does not exist, it will be created.'
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--topic',
            dest='topic',
            help='The name of the topic to use.  Will be generated if not provided.  Topic will be created if it does not exist.  This should be the last part of the colon (:) delimited arn.',
        ),
        make_option(
            '--region',
            dest='region',
            help='One of {0}'.format(VALID_REGIONS),
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
        if kwargs["json"]:
            log = lambda x: None
        else:
            log = lambda x: self.stdout.write(x)
        
        topic = kwargs["topic"] or "elastic-transcoder-{0}".format(uuid4())
        
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
                log('Region was not specified on the command line or on the settings module.  Proceeding without setting the sns region.  This is not recommended.')
        
        if region:
            sns.connect_to_region(region)
        
        #
        #    connect to sns
        #
        log('Creating sns connection')
        connection = sns.SNSConnection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        
        #
        #    retrieve sns topics
        #
        log('Retrieving all sns topics')
        
        topic_arns = []
        result = None
        token = None
        while result is None or token:
            response = connection.get_all_topics(token)
            result = response.get("ListTopicsResponse", {}).get("ListTopicsResult", {})
            token = result.get("NextToken", None)
            for r in result.get("Topics", []):
                topic_arns.append(r['TopicArn'])
        
        topics = {}
        for arn in topic_arns:
            name = arn.rsplit(":", 1)[-1]
            topics[name] = arn
        
        #
        #    create topic if it does not exist
        #
        if not topic in topics:
            log('Topic "%s" did not exist.' % topic)
            response = connection.create_topic(topic)
            result = response.get("CreateTopicResponse", {}).get("CreateTopicResult", {})
            arn = result["TopicArn"]
            topics[topic] = arn
            log('Created topic with arn "%s".' % arn)
        else:
            log('Topic already existed.  ARN is "%s".' % topics[topic])
        
        if kwargs["json"]: 
            return dumps({"name": topic, "arn": topics[topic]})