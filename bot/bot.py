# Copyright (C) 2015 Idein Inc.
# Author: koichi

import carnival
import re
import threading

#== Chat bot using carnival module ==

class Bot(carnival.ThreadingActor):
    def __init__(self, chat, name, icon_url = None):
        super().__init__(id=name)
        self._chat = chat
        self.name = name
        self._icon_url = icon_url
        self._actions = []

        self.listen('add_action', self._add_action)
        self.listen('chat:message', self._on_message)

        # Join this bot to the chat
        chat.add_bot(self)

    # post `text` to user `to` (if given) in `channel`
    def post(self, channel, text, to=None, **kwargs):
        self._chat.post(channel, self.name, text, to=to, icon_url=self._icon_url, **kwargs)
    
    # call `action` when `pattern` matches with received messages
    def hear(self, pattern, action, flag=0):
        self.send('add_action', {
            'pattern': pattern,
            'action': action,
            'flag': flag
            })

    def _add_action(self, mail):
        pat = re.compile(mail['pattern'], mail['flag'])
        self._actions.append((pat, mail['action']))

    def _on_message(self, mail):
        for p, h in self._actions:
            m = p.search(mail['text'])
            if m:
                ctx = ChatContext(self, m, mail)
                h(ctx, mail['text'])

class ChatContext(object):
    def __init__(self, bot, match, mail):
        self._bot   = bot
        self._match = match
        self._mail  = mail

    def post(self, text, to=None, **kwargs):
        text = self._match.expand(text)
        self._bot.post(self._mail['channel'], text, to=to, **kwargs)

    def reply(self, text, **kwargs):
        self.post(text, to=self._mail['user'], **kwargs)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._match.group(key)
        else:
            return self._mail[key]
