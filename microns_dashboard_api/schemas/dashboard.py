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
from microns_utils.datetime_utils import current_timestamp
from microns_utils.misc_utils import classproperty, unwrap
from microns_utils.widget_utils import SlackForWidget

from .. import base
from ..config import dashboard_config as config

config.register_externals()
config.register_adapters(context=locals())

schema = djp.schema(config.schema_name, create_schema=True)

slack_client = SlackForWidget(default_channel='#microns-dashboard')

os.environ['DJ_LOGLEVEL'] ='WARNING'
logger = djp.getLogger(__name__, level='WARNING', update_root_level=True)


version_attr = """-> Version \n"""
user_attr = """user : varchar(128) # dashboard username \n"""
info_type_attr = """info_type : varchar(128) \n"""

@schema
class Version(base.VersionLookup):
    package = 'microns-dashboard'


@schema
class Event(base.EventLookup):
    basedir = Path(config.externals.get('events').get('location'))
    additional_secondary_attrs = version_attr

    class UserInfoAdd(base.EventType):
        event_types = 'user_info_add'
        constant_attrs = {'version_id': Version.current_version_id}
        additional_primary_attrs = version_attr
        additional_secondary_attrs = user_attr + info_type_attr

        def on_event(self, event):
            UserInfo.Maker.populate({'event_id': event.id})

    class UserAccess(base.EventType):
        event_types = 'user_access'
        constant_attrs = {'version_id': Version.current_version_id}
        additional_primary_attrs = version_attr
        additional_secondary_attrs = user_attr

    class UserCheckIn(base.EventType):
        event_types = 'user_check_in'
        constant_attrs = {'version_id': Version.current_version_id}
        additional_primary_attrs = version_attr
        additional_secondary_attrs = (
            user_attr + \
            """check_in : tinyint # 1 if check in; 0 if check out \n"""
        )


@schema
class EventHandler(base.EventHandlerLookup):
    current_version_id = Version.current_version_id

    class UserEvent(djp.Part, dj.Lookup):
        enable_hashing = True
        hash_name = 'event_handler_id'
        hashed_attrs = 'version_id', 'event_type'
        definition = """
        -> master
        event_type : varchar(450) # type of event that method handles
        -> Version
        """

        @classproperty
        def contents(cls):
            for event_type in ['user_info_add']:
                cls.insert({'event_type': event_type, 'version_id': Version.current_version_id}, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)
            return {}

        def run(self, **key):
            event_type = self.fetch1('event_type')

            if event_type in ['user_info_add']:
                assert event_type == key['event_type'], f'event_type in handler {event_type} doesnt match event_type in key {key["event_type"]}'
                info_type = key.get('info_type')
                assert info_type is not None

                if info_type == 'slack_username':
                    username = slack_client.get_slack_username(key.get('data'))
                    assert username is not None, 'Slack usename not found.'
                    key['username'] = username
                
                return key


@schema
class UserInfo(djp.Lookup):
    definition = user_attr

    class Maker(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'ui_make_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        ui_make_id : varchar(10)
        -> Event
        -> EventHandler
        ---
        -> master
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        @classproperty
        def key_source(cls):
            return djp.U('event_id', 'event_handler_id') & Event.UserInfoAdd * EventHandler.UserEvent

        def make(self, key):
            key[self.hash_name] = self.hash1(key)
            try:
                key.update(Event.get1(key))
                key.update(EventHandler.run(key))
                self.insert1(key, ignore_extra_fields=True, insert_to_master=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True}, skip_hashing=True)
                
                if key.get('info_type') == 'slack_username':
                    self.master.Slack.insert1(key, ignore_extra_fields=True, skip_duplicates=True)
            except:
                key['traceback'] = traceback.format_exc()
                self.master.Attempted.insert1(key, insert_to_master=True, ignore_extra_fields=True)
                self.master.Log('exception', 'Error inserting Submission')
            

    class Attempted(djp.Part):
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        traceback=NULL : longblob
        ts_inserted_attempted=CURRENT_TIMESTAMP : timestamp # timestamp inserted into Attempted
        """

    class Slack(djp.Part):
        definition = f"""
        -> master
        ---
        username : varchar(450)
        ui_make_id : varchar(32)
        """

schema.spawn_missing_classes()
schema.connection.dependencies.load()
