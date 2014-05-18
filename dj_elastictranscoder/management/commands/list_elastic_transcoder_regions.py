from boto import elastictranscoder
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Lists valid elastic transcoder regions.'

    def handle(self, *args, **kwargs):
        for region in elastictranscoder.regions():
            self.stdout.write('%s' % region.name)