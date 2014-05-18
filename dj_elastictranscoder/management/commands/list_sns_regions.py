from boto import sns
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Lists valid sns regions.'

    def handle(self, *args, **kwargs):
        for region in sns.regions():
            self.stdout.write('%s' % region.name)