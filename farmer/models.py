#coding=utf8
from __future__ import with_statement

import os
import sys
import time
import json
import threading
from tempfile import mkdtemp
from datetime import datetime

from django.db import models
from django.utils import six

from farmer.settings import WORKER_TIMEOUT, ANSIBLE_FORKS
if six.PY3:
    from subprocess import getstatusoutput
else:
    from commands import getstatusoutput


class Task(models.Model):

    # hosts, like web_servers:host1 .
    inventories = models.TextField(null = False, blank = False)

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

    tmpdir = mkdtemp(prefix = 'ansible_', dir = '/tmp')

    @property
    def cmd_shell(self):
        option = self.sudo and '--sudo' or ''
        option += ' -f %s -m shell' % ANSIBLE_FORKS
        return 'ansible %s %s -a "%s"' % (self.inventories, option, self.cmd)

    def run(self):
        if os.fork() == 0:
        #if 0 == 0:

            self.start = datetime.now()
            self.save()

            # initial jobs
            cmd_shell = self.cmd_shell + ' --list-hosts'
            status, output = getstatusoutput(cmd_shell)

            hosts = map(str.strip, output.splitlines())

            for host in hosts:
                self.job_set.add(Job(host = host, cmd = self.cmd))
            self.save()

            # start a thread to monitor self.tmpdir
            t = threading.Thread(target = self.collect_result)
            t.setDaemon(True)
            t.start()

            # run ansible
            cmd_shell = self.cmd_shell + ' -t ' + self.tmpdir
            status, output = getstatusoutput(cmd_shell)

            # execution is perfect in WORKER_TIMEOUT seconds
            self.status = status
            self.save()

    def collect_result(self):

        now = time.time()

        while True:
            # timeout
            if time.time() - now > WORKER_TIMEOUT:
                for job in self.job_set.all():
                    if job.rc is None:
                        job.rc = 1
                        job.stderr = 'job timeout' # marked as 'TIMEOUT'
                        job.save()
                break

            # if there is none job whose rc is None: break
            if not filter(lambda job: job.rc is None, self.job_set.all()):
                break

            for f in os.listdir(self.tmpdir):
                with open(os.path.join(self.tmpdir, f)) as rf:
                    result = json.loads(rf.read())
                    job = self.job_set.get(host = f)
                    job.start = result.get('start')
                    job.end = result.get('end')
                    job.rc = result.get('rc')
                    job.stdout = result.get('stdout')
                    job.stderr = result.get('stderr')
                    job.save()
                    os.unlink(os.path.join(self.tmpdir, f)) # clean it
            
            time.sleep(1)

        if self.rc != 0 and filter(lambda job: job.rc or job.rc is None, self.job_set.all()):
            self.rc = 1
        else:
            self.rc = 0

        self.end = datetime.now()
        self.save()
        # clean tmp dir
        os.removedirs(self.tmpdir)
        sys.exit(self.rc)

    def __unicode__(self):
        return self.cmd_shell

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


