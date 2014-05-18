from boto import sns
from boto.exception import BotoServerError
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.core.urlresolvers import reverse
from json import loads
from optparse import make_option
from StringIO import StringIO

VALID_REGIONS = tuple([r.name for r in sns.regions()])

class Command(BaseCommand):
    help = 'Subscribes the encoder endpoint to an SNS topic for use with an Elastic Transcoder Pipeline.  If the topic does not exist, it will be created as well.'
    
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
            '--protocol',
            default="http",
            dest='protocol',
            help='"http" or "https", defaults to "http"',
        ),
        make_option(
            '--domain',
            dest='domain',
            help='e.g. "www.example.com" or "203.0.113.1:8080"',
        ),
        make_option(
            '--alias',
            default="",
            dest='alias',
            help='e.g. "/script/url/alias" if needed',
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
        domain = kwargs["domain"]
        
        if domain is None:
            raise CommandError("The 'domain' kwarg is required.")
        
        protocol = kwargs["protocol"] or "http"
        protocol = protocol.lower()
        if not protocol in ("http", "https"):
            raise CommandError("Invalid protocol specified.  You entered '%s'.  Protocol must be http or https.")
        
        topic = kwargs["topic"]
        alias = kwargs["alias"] or ""
        
        endpoint = "{0}://{1}{2}{3}".format(
            protocol,
            domain,
            alias,
            reverse("elastic-transcoder-endpoint"),
        )
        
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
                self.stdout.write('Region was not specified on the command line or on the settings module.  Proceeding without setting the sns region.  This is not recommended.')
        
        if region:
            sns.connect_to_region(region)
        
        #
        #    connect to sns
        #
        self.stdout.write('Creating sns connection')
        connection = sns.SNSConnection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        
        out = StringIO()
        call_command("create_encoder_topic", topic=topic, region=region, json=True, stdout=out)
        arn = loads(out.getvalue())["arn"]
        
        #
        #    subscribe endpoint
        #
        self.stdout.write('Subscribing %s' % endpoint)
        try:
            connection.subscribe(arn, protocol, endpoint)
        except BotoServerError, e:
            raise CommandError(
                'Attempting subscription raised the following exception:\n\tCode: {0}\n\tMessage: {1}'.format(
                    e.error_code,
                    e.message,
                )
            )
        self.stdout.write('Subscription is now pending confirmation.  Confirmation should occur automatically in just a moment.')