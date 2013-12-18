#coding=utf8
from __future__ import with_statement

import os
import sys
import time
import json
import threading
from tempfile import mkdtemp
from datetime import datetime

from ansible.runner import Runner
from ansible.inventory import Inventory

from django.db import models
from django.utils import six

from farmer.settings import WORKER_TIMEOUT, ANSIBLE_FORKS
if six.PY3:
    from subprocess import getstatusoutput
else:
    from commands import getstatusoutput


class Task(models.Model):

    # hosts, like web_servers:host1 .
    inventory = models.TextField(null = False, blank = False)

    # 0, do not use sudo; 1, use sudo .
    sudo = models.BooleanField(default = True)

    # for example: ansible web_servers -m shell -a 'du -sh /tmp'
    # the 'du -sh /tmp' is cmd here
    cmd = models.TextField(null = False, blank = False)

    # return code of this job
    rc = models.IntegerField(null = True)

    # submitter
    farmer = models.TextField(null = False, blank = False)

    start = models.DateTimeField(null = True, blank = False)
    end = models.DateTimeField(null = True, blank = False)

    def run(self):
        if os.fork() == 0:
        #if 0 == 0:

            self.start = datetime.now()
            self.save()

            # initial jobs
            for host in map(lambda i: i.name, Inventory().get_hosts(pattern = self.inventory)):
                self.job_set.add(Job(host = host, cmd = self.cmd, start = datetime.now()))
            self.save()

            runner = Runner(module_name = 'shell', module_args = self.cmd, \
                pattern = self.inventory, sudo = self.sudo)

            _, poller = runner.run_async(time_limit = 5)
            poller.wait(WORKER_TIMEOUT, poll_interval = 2)
            results = poller.results.get('contacted')

            for host, result in results.items():
                job = self.job_set.get(host = host)
                job.start = result.get('start')
                job.end = result.get('end')
                job.rc = result.get('rc')
                job.stdout = result.get('stdout')
                job.stderr = result.get('stderr')
                job.save()

            jobs_timeout = filter(lambda job: job.rc is None, self.job_set.all())
            jobs_failed = filter(lambda job: job.rc, self.job_set.all())

            for job in jobs_timeout:
                job.rc = 1
                job.stderr = 'TIMEOUT' # marked as 'TIMEOUT'
                job.save()

            self.rc = (jobs_timeout or jobs_failed) and 1 or 0

            self.end = datetime.now()
            self.save()

            sys.exit(self.rc)

    def __unicode__(self):
        return self.inventory + ' -> ' + self.cmd

class Job(models.Model):
    task = models.ForeignKey(Task)
    host = models.TextField(null = False, blank = False)
    cmd = models.TextField(null = False, blank = False)
    start = models.DateTimeField(null = True, blank = False)
    end = models.DateTimeField(null = True, blank = False)
    rc = models.IntegerField(null = True) 
    stdout = models.TextField(null = True)
    stderr = models.TextField(null = True)

    def __unicode__(self):
        return self.host + ' : ' + self.cmd


