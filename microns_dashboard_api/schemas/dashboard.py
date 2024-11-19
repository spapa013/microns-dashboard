"""
DataJoint tables for Dashboard Users.
"""
import json
import os
from pathlib import Path
import traceback

import datajoint as dj
import datajoint_plus as djp
import pandas as pd
import microns_utils.datajoint_utils as dju
from microns_utils.datetime_utils import current_timestamp
from microns_utils.misc_utils import classproperty, wrap, unwrap
from microns_utils.widget_utils import SlackForWidget

from ..config import dashboard_config as config

config.register_externals()
config.register_adapters(context=locals())

schema = djp.schema(config.schema_name, create_schema=True)

slack_client = SlackForWidget(default_channel='#microns-dashboard')

os.environ['DJ_LOGLEVEL'] ='WARNING'
logger = djp.getLogger(__name__, level='WARNING', update_root_level=True)


user_attr = """user : varchar(128) # dashboard username"""


@schema
class Tag(dju.VersionLookup):
    package = 'microns-dashboard'
    attr_name = 'tag'


@schema
class Event(dju.EventLookup):
    basedir = Path(config.externals.get('events').get('location'))
    extra_secondary_attrs = f"""
    -> {Tag.class_name}
    """

    class UserAccess(dju.Event):
        events = 'user_access'
        constant_attrs = {Tag.attr_name: Tag.version}
        extra_primary_attrs = f"""
        -> {Tag.class_name}
        """
        extra_secondary_attrs = user_attr
        def on_event(self, event):
            user, data = (self & {'event_id': event.id}).fetch1('user', 'data')
            if data is not None:
                entry_point = data.get('entry_point') if data.get('entry_point') is not None else 'dashboard'
            else:
                entry_point = 'dashboard'
            slack_client.post_to_slack(f'```{user} accessed the {entry_point} ```')                
    
    class UserCheckIn(dju.Event):
        events = 'user_check_in'
        constant_attrs = {Tag.attr_name: Tag.version}
        extra_primary_attrs = f"""
        -> {Tag.class_name}
        """
        extra_secondary_attrs = f"""
        {user_attr}
        check_in : tinyint # 1 if check in; 0 if check out
        """
        def on_event(self, event):
            user, check_in, data = (self & {'event_id': event.id}).fetch1('user', 'check_in', 'data')
            if data is not None:
                auto = data.get('auto')
            else:
                auto = False
            msg = f"```%s {'' if not auto else 'auto-'}checked {'in' if check_in else 'out'}```"
            slack_client.post_to_slack(msg % user)
            slack_client.post_to_slack(msg % 'You', channel=f'@{User.Slack.get_slack_username(user)}')

    class UserAdd(dju.Event):
        events = ['user_add', 'user_add_info']
        constant_attrs = {Tag.attr_name: Tag.version}
        extra_primary_attrs = f"""
        -> {Tag.class_name}
        """
        extra_secondary_attrs = f"""
        {user_attr}
        info_type=NULL : varchar(128)
        """
        def on_event(self, event):
            if event.name == 'user_add':
                User.Add.populate({'event_id': event.id})
                user = (self & {'event_id': event.id}).fetch1('user')
                slack_client.post_to_slack(f"```{user} was added to the dashboard```")
            
            elif event.name == 'user_add_info':
                User.AddInfo.populate({'event_id': event.id})
                user, info_type = (self & {'event_id': event.id}).fetch1('user', 'info_type')
                msg = f"```%s updated %s {' '.join(info_type.split('_'))}```"
                slack_client.post_to_slack(msg % (user, 'their'))
                slack_client.post_to_slack(msg % ('You', 'your'), channel=f'@{User.Slack.get_slack_username(user)}')


@schema
class EventHandler(dju.EventHandlerLookup):
    @classmethod
    def run(cls, key):
        handler = cls.r1p(key)
        handler_event = handler.fetch1('event')
        handler_version = handler.fetch1(Tag.attr_name)
        cls.Log('info',  'Running %s', handler.class_name)
        cls.Log('debug', 'Running %s with key %s', handler.class_name, key)
        assert handler_event == key['event'], f'event in handler {handler_event} doesnt match event in key {key["event"]}'
        assert handler_version == Tag.version, f'version mismatch, event_handler version_id is {handler_version} but the current version_id is {Tag.version}'
        key = handler.run(key)
        cls.Log('info', '%s ran successfully.', handler.class_name)
        return key

    class UserEvent(dju.EventHandler):
        constant_attrs = {Tag.attr_name: Tag.version}
        hashed_attrs = 'event', Tag.attr_name
        extra_primary_attrs = f"""
        -> {Tag.class_name}
        """

        @classproperty
        def contents(cls):
            for event in Event.UserAdd.events:
                key = {'event': event}
                key.update(cls.constant_attrs)
                cls.insert(key, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)
            return {}

        def run(self, key):
            event = self.fetch1('event')

            if event in ['user_add']:
                return key
            
            if event in ['user_add_info']:
                info_type = key.get('info_type')
                
                if info_type == 'slack_username':
                    username = slack_client.get_slack_username(key.get('data'))
                    assert username is not None, 'Slack username not found.'
                    key[info_type] = username
                
                return key


@schema
class User(djp.Lookup):
    hash_name = 'make_id'
    definition = f"""
    {user_attr}
    ---
    make_id : varchar(10)
    timestamp=CURRENT_TIMESTAMP : timestamp
    """

    class Add(dju.Maker):
        hash_name = 'make_id'
        upstream = Event
        method = EventHandler
        events = 'user_add'
        definition = """
        -> master
        -> Event
        -> EventHandler
        make_id : varchar(10)
        """
        @classproperty
        def key_source(cls):
            return (djp.U('event_id', 'event_handler_id') & ((Event.UserAdd & [{'event': e} for e in wrap(cls.events)]) * EventHandler.UserEvent))

    class AddInfo(dju.Maker):
        hash_name = 'make_id'
        upstream = Event
        method = EventHandler
        events = 'user_add_info'
        definition = """
        -> master
        -> Event
        -> EventHandler
        make_id : varchar(10)
        """
        @classproperty
        def key_source(cls):
            return (djp.U('event_id', 'event_handler_id') & ((Event.UserAdd & [{'event': e} for e in wrap(cls.events)]) * EventHandler.UserEvent))

        def on_make(self, key):
            if key.get('info_type') == 'slack_username':
                self.master.Slack.insert1(key, ignore_extra_fields=True, replace=True)

    class Slack(djp.Part):
        store = True
        definition = f"""
        -> master
        ---
        slack_username : varchar(450)
        make_id : varchar(10)
        """

schema.spawn_missing_classes()

