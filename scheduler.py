# Copyright (C) 2015 Idein Inc.
# Author: koichi

import carnival
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from datetime import timedelta, datetime
from math import ceil

def fire(to, tag, mail):
    carnival.send(to, tag or 'fire', mail)

class Scheduler(carnival.ThreadingActor):
    def __init__(self, timezone = 'UTC', id=None):
        super().__init__(id=id)

        self.sched = BackgroundScheduler({
            'apscheduler.jobstores.default': {
                'type': 'sqlalchemy',
                'url': 'sqlite:///jobs.sqlite'
            },
            'apscheduler.executors.default': {
                'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                'max_workers': '20'
            },
            'apscheduler.job_defaults.coalesce': 'false',
            'apscheduler.timezone': timezone,
            })
        self.sched.start()

        self.listen('sched:register', self._register)
        self.listen('sched:unregister', self._unregister)

    def _register(self, mail):
        to    = mail.pop('to', None)
        tag   = mail.pop('tag', None)
        reply = mail.pop('mail', None)

        self.sched.add_job(fire, args=(to, tag, reply), **mail)

    def _unregister(self, mail):
        self.sched.remove_job(mail['id'])

    def register(self, to, tag, mail, schedule):
        if to.id is None:
            raise RuntimeError('Can not register schedule %s for anonymous actor: %s' % (tag, str(to)))
        schedule['to'] = to.id
        schedule['tag'] = tag
        schedule['mail'] = mail
        self.send('sched:register', schedule)

    def unregister(self, id):
        self.send('sched:unregister', {'id': id})

    def schedule(self, to, tag=None, mail=None, date=None, timezone=None):
        self.register(self, to, tag, mail, {
            'trigger': 'date',
            'run_date': date,
            'timezone': timezone
            })

    # timer_args:
    #   weeks, days, hours, minutes, seconds: (int)
    def timer(self, to, tag=None, mail=None, timezone=None, **timer_args):
        date = datetime.now() + timedelta(**timer_args)
        self.register(to, tag, mail, {
            'trigger': 'date',
            'run_date': date,
            'timezone': timezone
            })

    # schedule:
    #   weeks, days, hours, minutes, seconds: (int)
    #   start_date, end_date: (str or datetime)
    #   timezone: (str or datetime.tzinfo)
    def interval(self, to, tag=None, mail=None, **schedule):
        self.register(to, tag, mail, dict(schedule, trigger='interval'))

    # cron_args:
    #   year, month, day, week, day_of_week, hour, minute, second: (int or str)
    #   start_date, end_date: (str of datetime)
    #   timezone: (str or datetime.tzinfo)
    # 
    # expression:
    #   *	any	Fire on every value
    #   */a	any	Fire every a values, starting from the minimum
    #   a-b	any	Fire on any value within the a-b range (a must be smaller than b)
    #   a-b/c	any	Fire every c values within the a-b range
    #   xth y	day	Fire on the x -th occurrence of weekday y within the month
    #   last x	day	Fire on the last occurrence of weekday x within the month
    #   last	day	Fire on the last day within the month
    #   x,y,z	any	Fire on any matching expression; can combine any number of any of the above expressions
    #
    def cron(self, to, tag=None, mail=None, **cron_args):
        self.register(to, tag, mail, dict(cron_args, trigger='cron'))
