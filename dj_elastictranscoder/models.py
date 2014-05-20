from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.generic import GenericForeignKey


class EncodeJob(models.Model):
    STATE_SUBMITTED = 0
    STATE_PROGRESSING = 1
    STATE_ERROR = 2
    STATE_COMPLETE = 3
    STATE_CHOICES = (
        (STATE_SUBMITTED, 'Submitted'),
        (STATE_PROGRESSING, 'Progressing'),
        (STATE_ERROR, 'Error'),
        (STATE_COMPLETE, 'Complete'),
    )
    ACTIVE_STATES = (STATE_SUBMITTED, STATE_PROGRESSING)
    
    id = models.CharField(max_length=100, primary_key=True)
    content_type = models.ForeignKey(ContentType)
    object_id = models.PositiveIntegerField()
    state = models.PositiveIntegerField(choices=STATE_CHOICES, default=0, db_index=True)
    content_object = GenericForeignKey()
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    last_modified = models.DateTimeField(auto_now=True)
