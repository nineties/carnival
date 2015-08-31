# Copyright (C) 2015 Idein Inc.
# Author: koichi

#== Implementation of Actor model ==
# This module provides two kinds of implementations: threads and processes.

import sys
import threading
import queue
import uuid
import multiprocessing
from carnival.logging import logger

_QUEUE_TIMEOUT = 5 # timeout of fetching message from queues

class Future(object):
    def __init__(self, func):
        self._value = None
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._set, args=(func,))
        self._thread.start()

    def _set(self, func):
        v = func()
        with self._lock:
            self._value = v

    def get(self, timeout=None):
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise RuntimeError('%s: timeout', self)
        return self._value

    def wait(self, timeout=None):
        self.get(timeout)

# registry of all actors. thread safe.
class Registry(object):
    _dict = {}
    _lock = threading.Lock()

    @classmethod
    def registor(cls, actor):
        with cls._lock:
            if actor.id in cls._dict:
                raise RuntimeError('Duplicated actor ID', actor.id)
            cls._dict[actor.id] = actor
        logger.debug('Registered %s', actor)

    @classmethod
    def unregistor(cls, actor):
        with cls._lock:
            if actor.id in cls._dict:
                del cls._dict[actor.id]
                logger.debug('Unregistered %s', actor)

    @classmethod
    def get(cls, id):
        with cls._lock:
            return cls._dict.get(id, None)

    @classmethod
    def getall(cls, klass=None):
        with cls._lock:
            if klass is None:
                return list(cls._dict.values())
            else:
                return [actor for actor in cls._dict.values()
                        if isinstance(actor, klass)]

    @classmethod
    def broadcast(cls, tag, mail=None, klass=None):
        actors = cls.getall(klass=klass)
        for actor in actors:
            actor.send(tag, mail)

    @classmethod
    def stopall(cls, klass=None):
        actors = cls.getall(klass)
        for actor in actors:
            actor._actor_stop()

def get(id):
    return Registry.get(id)

def getall(klass=None):
    return Registry.getall(klass)

def getall(klass=None):
    return Registry.getall(klass)

def broadcast(tag, mail=None, klss=None):
    Registry.broadcast(tag, mail, klass)

def stopall(klass=None):
    Registry.stopall(klass)

# The base class of all actor objects.
# **Do not instantiate this class directly.**
class Actor(object):
    def __init__(self, id=None):
        self.id = id
        self._mailbox = self._create_mailbox()
        self._running = threading.Event()
        self._handlers = {
            'actor:listen': self._listen,
            'actor:unlisten': self._unlisten
            }
        if id is not None:
            Registry.registor(self)

    def listen(self, tag, handler):
        self.send('actor:listen', {'tag': tag, 'handler': handler})

    def unlisten(self, tag):
        self.send('actor:unlisten', {'tag': tag})

    def send(self, tag, mail=None):
        if not self._running.is_set():
            self._start_actor()
        self._mailbox.put((tag, mail))

    def request(self, tag, mail=None, timeout=None):
        if not self.id:
            raise RuntimeError('Anonymous actor cant send a request', self)
        mailbox = self._create_mailbox(1)
        id = uuid.uuid4().urn
        reply_tag = 'reply:' + id

        def _set(mail):
            mailbox.put(mail['value'])

        self.listen(reply_tag, _set)
        self.send(tag, dict(mail or [], reply_to=self.id, reply_tag=reply_tag))

        def _get():
            try:
                value = mailbox.get(block=True, timeout=timeout)
            except queue.Empty:
                value = None
            self.unlisten(reply_tag)
            return value

        return Future(_get)

    def _listen(self, mail):
        self._handlers[mail['tag']] = mail['handler']

    def _unlisten(self, mail):
        tag = mail['tag']
        if tag in self._handlers:
            del self._handlers[tag]

    def on_resume(self):
        pass

    def on_suspend(self):
        pass

    def on_fail(self, exc_type, exc_value, traceback):
        pass

    def _start_actor(self):
        if self._running.is_set():
            return
        self._running.set()
        self._start_loop()

    def _actor_stop(self):
        if self._running.is_set():
            self.send('actor:stop')

    def _main_loop(self):
        logger.debug('Start %s', self)
        self.on_resume()
        while True:
            try:
                tag, mail = self._mailbox.get(block=True, timeout=_QUEUE_TIMEOUT)
                if tag == 'actor:stop':
                    break

                reply_to = reply_tag = None
                if mail:
                    reply_to  = mail.pop('reply_to', None)
                    reply_tag = mail.pop('reply_tag', None)

                response = self._handle(tag, mail)

                if reply_to and reply_tag:
                    send(reply_to, reply_tag, {'value': response})
            except queue.Empty:
                break
            except Exception:
                self._fail(*sys.exc_info())
                try:
                    self.on_fail(*sys.exc_info())
                except Exception:
                    self._fail(*sys.exc_info())
        self._running.clear()
        self.on_suspend()
        logger.debug('Suspended %s', self)

    def _handle(self, tag, mail):
        h = self._handlers.get(tag)
        if h:
            return h(mail)

    def _fail(self, exc_type, exc_value, traceback):
        logger.error(
            'Unhandled exception in %s:' % self,
            exc_info=(exc_type, exc_value, traceback))

    def __str__(self):
        if self.id:
            return '%s (%s)' % (self.__class__.__name__, self.id)
        else:
            return '%s' % self.__class__.__name__

    def __repr__(self):
        return '<Actor: %s>' % str(self)

# Send a message using actor's id
def send(to, tag, mail=None):
    actor = Registry.get(to)
    if actor:
        actor.send(tag, mail)
    else:
        logger.error('Actor not found %s:' % to)

# Thread implementation
class ThreadingActor(Actor):
    def _create_mailbox(self, max_size=0):
        return queue.Queue(max_size)

    def _start_loop(self):
        threading.Thread(target=self._main_loop).start()

# Process implementation

class ProcessActor(Actor):
    def _create_mailbox(self, max_size=0):
        return multiprocessing.Queue(max_size)

    def _start_loop(self):
        multiprocessing.Process(target=self._main_loop).start()
