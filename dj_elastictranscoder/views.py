import json
import logging

from django.http import HttpResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.core.mail import mail_admins
from urllib2 import urlopen

from .models import EncodeJob
from .signals import (
    transcode_onprogress,
    transcode_onerror,
    transcode_oncomplete
)

logger = logging.getLogger("dj_elastictranscoder.views")

@csrf_exempt
def endpoint(request):
    """
    Receive SNS notification
    """
    request_data = request.read()
    try:
        try:
            data = json.loads(request_data)
        except ValueError:
            return HttpResponseBadRequest('Invalid JSON')
    
    
        # handle SNS subscription
        if data['Type'] == 'SubscriptionConfirmation':
            subscribe_url = data['SubscribeURL']
            
            subscribe_body = """
            This message serves as a fail-safe in case the automatic subscription confirmation fails. 
            
            Please visit this URL below to confirm your subscription with SNS
    
            %s """ % subscribe_url
    
            mail_admins('Please confirm SNS subscription', subscribe_body)
            urlopen(subscribe_url)
            return HttpResponse('OK')
    
        
        #
        try:
            message = json.loads(data['Message'])
        except ValueError:
            assert False, data['Message']
    
        #
        if message['state'] == 'PROGRESSING':
            job = EncodeJob.objects.get(pk=message['jobId'])
            job.message = 'Progress'
            job.state = job.STATE_PROGRESSING
            job.save()
    
            transcode_onprogress.send(sender=None, job=job, message=message)
        elif message['state'] == 'COMPLETED':
            job = EncodeJob.objects.get(pk=message['jobId'])
            job.message = 'Success'
            job.state = job.STATE_COMPLETE
            job.save()
    
            transcode_oncomplete.send(sender=None, job=job, message=message)
        elif message['state'] == 'ERROR':
            job = EncodeJob.objects.get(pk=message['jobId'])
            try:
                job.message = message['messageDetails']
            except KeyError:
                details = []
                for output in message["outputs"]:
                    details.append(output["statusDetail"])
                job.message = json.dumps(details)
            job.state = job.STATE_ERROR
            job.save()
    
            transcode_onerror.send(sender=None, job=job, message=message)
    
        return HttpResponse('Done')
    except Exception, e:
        logger.exception("'%s' exception was raised processing the endpoint view. Posted data was as follows: '%s'" % (e.__class__.__name__, request_data))
        raise