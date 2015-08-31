# Copyright (C) 2015 Idein Inc.
# Author: koichi

import carnival

class Chat(carnival.ThreadingActor):
    def __init__(self, id=None):
        super().__init__(id=id)
        self._bots = []

        self.listen('chat:post', self._post)
        self.listen('chat:channel_list', self._channel_list)
        self.listen('chat:user_list', self._user_list)
        self.listen('chat:add_bot', self._add_bot)
        self.listen('chat:remove_bot', self._remove_bot)
        self.listen('chat:deliver', self._deliver)

    def add_bot(self, bot):
        self.send('chat:add_bot', {'bot': bot})

    def remove_bot(self, bot):
        self.send('chat:remove_bot', {'bot': bot})

    def post(self, channel, user, text, **kwargs):
        self.send('chat:post', dict(kwargs,
            channel = channel,
            user = user,
            text = text
            ))

    # post mail['text'] to mail['channel'].
    def _post(self, mail):
        raise NotImplementedError('chat:post')

    def _channel_list(self, mail):
        raise NotImplementedError('chat:channel_list')

    def _user_list(self, mail):
        raise NotImplementedError('chat:user_list')

    def _add_bot(self, mail):
        self._bots.append(mail['bot'])

    def _remove_bot(self, mail):
        if mail['bot'] in self._bots:
            self._bots.remove(mail['bot'])

    # deliver messages to bots
    def _deliver(self, mail):
        tag  = mail['tag']
        mail = mail['mail']
        for bot in self._bots:
            bot.send(tag, mail)

    def deliver(self, tag, mail):
        self.send('chat:deliver', {'tag': tag, 'mail': mail})
