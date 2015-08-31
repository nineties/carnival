# Copyright (C) 2015 Idein Inc.
# Author: koichi

from carnival import ThreadingActor, WebSocket, Scheduler
from carnival.chat import Chat
from carnival.logging import logger
import slacker
import json
import re

# Wrapper of slacker
SLACK_API_METHODS = [
    "api.test", "auth.test", "channels.archive", "channels.create",
    "channels.history", "channels.info", "channels.invite",
    "channels.join", "channels.kick", "channels.leave", "channels.list",
    "channels.mark", "channels.rename", "channels.set_purpose",
    "channels.set_topic", "channels.unarchive", "chat.delete",
    "chat.post_message", "chat.update", "emoji.list", "files.delete",
    "files.info", "files.list", "files.upload", "groups.archive",
    "groups.close", "groups.create", "groups.create_child",
    "groups.history", "groups.info", "groups.invite", "groups.kick",
    "groups.leave", "groups.list", "groups.mark", "groups.open",
    "groups.rename", "groups.set_purpose", "groups.set_topic",
    "groups.unarchive", "im.close", "im.history", "im.list", "im.mark",
    "im.open", "oauth.access", "rtm.start", "search.all", "search.files",
    "search.messages", "stars.list", "team.access_logs", "team.info",
    "users.get_presence", "users.info", "users.list", "users.set_active",
    "users.set_presence"]

RTM_EVENT_TYPES = [
    "hello", "message", "user_typing", "channel_marked", "channel_created",
    "channel_joined", "channel_left", "channel_deleted", "channel_rename",
    "channel_archive", "channel_unarchive", "channel_history_changed",
    "im_created", "im_open", "im_close", "im_marked", "im_history_changed",
    "group_joined", "group_left", "group_open", "group_close",
    "group_archive", "group_unarchive", "group_rename", "group_marked",
    "group_history_changed", "file_created", "file_shared",
    "file_unshared", "file_public", "file_private", "file_change",
    "file_deleted", "file_comment_added", "file_comment_edited",
    "file_comment_deleted", "pin_added", "pin_removed", "presence_change",
    "manual_presence_change", "pref_change", "user_change", "team_join",
    "star_added", "star_removed", "emoji_changed", "commands_changed",
    "team_plan_change", "team_pref_change", "team_rename",
    "team_domain_change", "email_domain_changed", "bot_added",
    "bot_changed", "accounts_changed", "team_migration_started",
    ]

class SlackAPI(ThreadingActor):
    def __init__(self, token):
        super().__init__()

        s = slacker.Slacker(token)
        for method in SLACK_API_METHODS:
            cat, meth = method.split('.')
            func = getattr(getattr(s, cat), meth)

            # call a method to create a scope for `func`
            self._add_method(method, func)

    def _add_method(self, method, func):
        def _callback(mail):
            return func(**mail).body
        self.listen(method, _callback)

# wait times in sec. for reconnecting to slack
# when some error happens.
SLACK_RESTART_WAITTIMES = [0, 1, 10, 60, 300, 600]
class Slack(Chat):
    def __init__(self, token, scheduler=None, id='Slack'):
        self._api = SlackAPI(token)
        self._env = None
        self._ws = None
        self._retry = 0
        self._sched = scheduler or Scheduler()
        self._event_handlers = {}
        for ty in RTM_EVENT_TYPES:
            self._event_handlers[ty] = getattr(self, '_'+ty)

        # launch chat room after initialization
        super().__init__(id=id)

        self.listen('slack:connect', self._connect)
        self.listen('ws:receive', self._receive)
        self.send('slack:connect')

    def _post(self, mail):
        # setup parameters
        # See https://api.slack.com/methods/chat.postMessage for details.
        params = {}
        params['channel']      = mail['channel']
        params['text']         = mail['text']
        params['username']     = mail.get('user', 'No name')
        params['as_user']      = mail.get('as_user', False)
        params['parse']        = mail.get('parse', 'full')
        params['link_names']   = mail.get('link_names', 1)
        params['attachments']  = mail.get('attachments')
        params['unfurl_links'] = mail.get('unfurl_links', True)
        params['unfurl_media'] = mail.get('unfurl_media', True)
        params['icon_url']     = mail.get('icon_url')
        params['icon_emoji']   = mail.get('icon_emoji')

        to = mail.get('to')
        if to:
            params['text'] = '@%s %s' % (to, params['text'])
        params['link_names'] = 1
        self._api.send('chat.post_message', params)

    def on_fail(self, exc_type, exc_value, traceback):
        if self._env:
            self._env = None
        if self._ws:
            self._ws.send('ws:stop')
            self._ws = None
        self._sched.timer(self, 'slack:connect',
            seconds = SLACK_RESTART_WAITTIMES[self._retry])

    def _connect(self, mail):
        logger.info('%s connecting to Slack', ["Retry","Try"][self._retry==0])

        if self._retry + 1 < len(SLACK_RESTART_WAITTIMES):
            self._retry += 1

        info = self._api.request('rtm.start').get()

        self._env = {
            'team': info['team'],
            'users': {user['id']: user for user in info['users']},
            'channels': {chan['id']: chan for chan in info['channels']},
            'groups': {group['id']: group for group in info['groups']},
            'ims': {im['id']: im for im in info['ims']},
        }

        self._ws = WebSocket(info['url'])

        # make sure to complete adding Slack as a consumer of the websocket
        # before start it.
        self._ws.request('ws:add_consumer', {'consumer': self}).wait(1)
        self._ws.send('ws:start')

    def _receive(self, packet):
        mail = json.loads(packet['data'])
        ty = mail.pop('type', None)
        if ty is None:
            logger.info('Unknown Slack event: %s', packet['data'])
            return

        h = self._event_handlers.get(ty)
        if h is None:
            logger.info('Unknown Slack event: %s', packet['data'])
            return

        h(mail)
        self.deliver(ty, mail)

    def get_user_name(self, id):
        return self._env['users'][id]['name']

    def get_channel_name(self, id):
        if id[0] == 'C':
            return '#' + self._env['channels'][id]['name']
        elif id[0] == 'G':
            return '#' + self._env['groups'][id]['name']
        else:
            return id
    
    def _unescape(self, m):
        if m.group(2):
            return '@' + self.get_user_name(m.group(2))
        elif m.group(5):
            return '#' + self.get_channel_name(m.group(5))
        else:
            return m.group(7)

    # slack's formatted texts to readable texts 
    def _unescape_text(self, text):
        # See https://api.slack.com/docs/formatting
        return re.sub(r"<(@(\w+)(\|[^>]+)?)|(#(\w+)(\|[^>]+)?)|(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)>",
                self._unescape, text).replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


    # Event handlers
    def _hello(self, mail):
        logger.info('Successfully connected to %s\'s Slack', self._env['team']['name'])
        self._retry = 0

    # Unescape values, usernames
    def _message(self, mail):
        mail = dict(mail)
        if 'subtype' in mail:
            return
        mail['text'] = self._unescape_text(mail['text'])
        mail['user']    = self.get_user_name(mail['user'])
        mail['channel'] = self.get_channel_name(mail['channel'])
        self.deliver('chat:message', mail)

    def _user_typing(self, mail):
        pass

    def _channel_marked(self, mail):
        channel = self._env['channels'].get(mail['channel'])
        if channel:
            channel['last_read'] = message['ts']

    def _channel_created(self, mail):
        channel = mail['channel']
        self._env['channels'][channel['id']] = channel

    def _channel_joined(self, mail):
        channel = mail['channel']
        self._env['channels'][channel['id']] = channel

    def _channel_left(self, mail):
        self._env['channels'].pop(mail['channel'], None)

    def _channel_deleted(self, mail):
        self._env['channels'].pop(mail['channel'], None)

    def _channel_rename(self, mail):
        channel = self._env['channels'].get(mail['channel']['id'])
        if channel:
            channel['name'] = mail['channel']['name']

    def _channel_archive(self, mail):
        channel = self._env['channels'].get(mail['channel'])
        if channel:
            channel['is_archived'] = True

    def _channel_unarchive(self, mail):
        channel = self._env['channels'].get(mail['channel'])
        if channel:
            channel['is_archived'] = False

    def _channel_history_changed(self, mail):
        pass

    def _im_created(self, mail):
        im = mail['channel']
        self._env['ims'][im['id']] = im

    def _im_open(self, mail):
        pass

    def _im_close(self, mail):
        pass

    def _im_marked(self, mail):
        im = self._env['ims'].get(mail['channel'])
        if im:
            im['last_read'] = message['ts']

    def _im_history_changed(self, mail):
        pass

    def _group_joined(self, mail):
        group = mail['group']
        self._env['groups'][group['id']] = group

    def _group_left(self, mail):
        self._env['groups'].pop(mail['channel'], None)

    def _group_open(self, mail):
        pass

    def _group_close(self, mail):
        pass

    def _group_archive(self, mail):
        group = self._env['groups'].get(mail['channel'])
        if group:
            group['is_archived'] = True

    def _group_unarchive(self, mail):
        group = self._env['groups'].get(mail['channel'])
        if group:
            group['is_archived'] = False

    def _group_rename(self, mail):
        group = self._env['groups'].get(mail['channel']['id'])
        if group:
            group['name'] = mail['channel']['name']

    def _group_marked(self, mail):
        group = self._env['groups'].get(mail['channel'])
        if group:
            group['last_read'] = message['ts']

    def _group_history_changed(self, mail):
        pass

    def _file_created(self, mail):
        pass

    def _file_shared(self, mail):
        pass

    def _file_unshared(self, mail):
        pass

    def _file_public(self, mail):
        pass

    def _file_private(self, mail):
        pass

    def _file_change(self, mail):
        pass

    def _file_deleted(self, mail):
        pass

    def _file_comment_added(self, mail):
        pass

    def _file_comment_edited(self, mail):
        pass

    def _file_comment_deleted(self, mail):
        pass

    def _pin_added(self, mail):
        pass

    def _pin_removed(self, mail):
        pass

    def _presence_change(self, mail):
        pass

    def _manual_presence_change(self, mail):
        pass

    def _pref_change(self, mail):
        pass

    def _user_change(self, mail):
        user = mail['user']
        self._env['users'][user['id']] = user

    def _team_join(self, mail):
        user = mail['user']
        self._env['users'][user['id']] = user

    def _star_added(self, mail):
        pass

    def _star_removed(self, mail):
        pass

    def _emoji_changed(self, mail):
        pass

    def _commands_changed(self, mail):
        pass

    def _team_plan_change(self, mail):
        pass

    def _team_pref_change(self, mail):
        name = message['name']
        value = message['value']
        self._env['team']['prefs'][name] = value

    def _team_rename(self, mail):
        self._env['team']['name'] = message['name']

    def _team_domain_change(self, mail):
        self._env['team']['domain'] = message['domain']

    def _email_domain_changed(self, mail):
        self._env['team']['email_domain'] = message['email_domain']

    def _bot_added(self, mail):
        pass

    def _bot_changed(self, mail):
        pass

    def _accounts_changed(self, mail):
        pass

    def _team_migration_started(self, mail):
        pass

