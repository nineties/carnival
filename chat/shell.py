# Copyright (C) 2015 Idein Inc.
# Author: koichi

from carnival.chat import Chat
import threading
import time
import os

class Shell(Chat):
    def __init__(self, id='Shell'):
        super().__init__(id=id)
        self._reading = threading.Event()
        self._prompt = '(%s) ' % os.environ['USER']
        thread = threading.Thread(target=self._receive)
        thread.start()

    # Override
    def add_bot(self, bot):
        self.display('>> %s is joined' % bot.name)
        super().add_bot(bot)

    # Override
    def remove_bot(self, bot):
        self.display('>> %s is left' % bot.name)
        super().remove_bot(bot)

    def display(self, text):
        if self._reading.is_set():
            print('')
        print(text)
        if self._reading.is_set():
            print(self._prompt, end="", flush=True)

    def _receive(self):
        time.sleep(0.1)
        while True:
            self._reading.set()
            line = input(self._prompt).rstrip()
            self._reading.clear()
            if line is None or line == 'exit':
                break
            if line == '':
                continue
            self.deliver('chat:message', {
                'user': os.environ['USER'],
                'channel': '-',
                'text': line
                })

            time.sleep(0.1) # wait response

    def _post(self, mail):
        to = mail.get('to')
        if to:
            self.display('(%s) @%s %s' % (mail['user'], to, mail['text']))
        else:
            self.display('(%s) %s' % (mail['user'], mail['text']))
