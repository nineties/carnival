# Copyright (C) 2015 Idein Inc.
# Author: koichi

# watch given websocket url and send messages to registered consumers.

import websockets
import carnival
import asyncio

class WebSocket(carnival.ThreadingActor):
    def __init__(self, url):
        super().__init__()
        self._url = url
        self._ws = None
        self._loop = None
        self._consumers = []

        self.listen('ws:start', self._start)
        self.listen('ws:stop', self._stop)
        self.listen('ws:add_consumer', self._add_consumer)
        self.listen('ws:remove_consumer', self._remove_consumer)

    def on_fail(self, exc_type, exc_value, traceback):
        self._cleanup()

    def _start(self, mail):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._watch_loop())

    def _stop(self, mail):
        self._cleanup()

    def _cleanup(self):
        if self._loop:
            self._loop.close()
            self._loop = None
        if self._ws:
            self._ws.close()
            self._ws = None
        self._consumers = []

    def _add_consumer(self, mail):
        self._consumers.append(mail['consumer'])

    def _remove_consumer(self, mail):
        if mail['consumer'] in self._consumers:
            self._consumers.remove(mail['consumer'])

    def add_consumer(self, actor):
        self.send('add_consumer', {'consumer': actor})

    def remove_consumer(self, actor):
        self.send('remove_consumer', {'consumer': actor})

    @asyncio.coroutine
    def _watch_loop(self):
        self._ws = yield from websockets.connect(self._url)
        while True:
            message = yield from self._ws.recv()
            if message is None:
                break
            for bot in self._consumers:
                bot.send('ws:receive', {'data': message})
        self._ws = None
