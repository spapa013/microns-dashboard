"""
DataJoint tables for Dashboard Users.
"""
import json
import os
from pathlib import Path

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


@schema
class Version(base.VersionLookup):
    package = 'microns-dashboard'

version_id = Version.current_version_id
version_id_dict = {'version_id': Version.current_version_id}

@schema
class Event(base.EventLookup):
    basedir = Path(config.externals.get('events').get('location'))
    additional_secondary_attrs = """
    -> Version
    """
    @staticmethod
    def on_event(event, processing_table, destination_table=None):
        event_id_restr = {'event_id': event.id}
        processing_table.populate(event_id_restr)
        if destination_table is not None:
            processing_table_restr = processing_table & event_id_restr
            if len(processing_table_restr) == 1:
                destination_table.populate({'processed_id': processing_table_restr.fetch1('processed_id')})

    class TestEvent(base.EventType):
        event_type = 'test_event'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.TestEvent
            )

    class UserAdd(base.EventType):
        event_type = 'user_add'
        required_keys = 'user'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.UserAdd, 
                destination_table=User.Username
            )

    class UserInfoAdd(base.EventType):
        event_type = 'user_info_add'
        required_keys = 'user', 'info_type', 'info'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.UserInfoAdd, 
                destination_table=User.Info
            )

    class UserAccess(base.EventType):
        event_type = 'user_access'
        required_keys = 'user'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.UserAccess, 
                destination_table=User.Accessed
            )

    class UserCheckIn(base.EventType):
        event_type = 'user_check_in'
        required_keys = 'user'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.UserCheckIn, 
                destination_table=User.CheckInOut
            )

    class UserCheckOut(base.EventType):
        event_type = 'user_check_out'
        required_keys = 'user'
        constant_attrs = version_id_dict
        additional_primary_attrs = """
        -> Version
        """
        def on_event(self, event):
            self.master.on_event(
                event, 
                processing_table=ProcessedEvent.UserCheckOut, 
                destination_table=User.CheckInOut
            )
        

@schema
class EventHandler(djp.Lookup):
    hash_name = 'event_handler_id'
    definition = """
    event_handler_id : varchar(6) # id of event handler
    """

    @classmethod
    def run(cls, data, **key):
        handler = cls.r1p(key)
        event_type = handler.fetch1('event_type')
        handler_version_id = handler.fetch1('version_id')
        cls.Log('info', 'Running %s with event_type: %s', handler.class_name, event_type)
        cls.Log('debug', 'Running %s with key %s', handler.class_name, key)
        assert  handler_version_id == version_id, f'Version mismatch, event_handler version_id is {handler_version_id} but the current version_id is {version_id}'
        pdata = handler.run(data, **key)
        cls.Log('info', '%s and event_type: %s ran successfully.', handler.class_name, event_type)
        return pdata

    class TestEvent(djp.Part, dj.Lookup):
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
            for event_type in list(Event.types()):
                if event_type.split('_')[0] == 'test':
                    cls.insert(Version, constant_attrs={'event_type': event_type}, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)
            return {}

        def run(self, data, **kwargs):
            params = self.fetch1()
            
            if params.get('event_type') == 'test_event':
                return data

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
            for event_type in list(Event.types()):
                if event_type.split('_')[0] == 'user':
                    cls.insert(Version, constant_attrs={'event_type': event_type}, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)
            return {}

        def run(self, data, **kwargs):
            event_type = self.fetch1('event_type')
            user = data.get('user')

            if event_type in ['user_add']:
                return {'user': user}
            
            if event_type in ['user_access']:
                event_ts = Event.r1pwh(kwargs.get('event_id')).fetch1('event_ts')
                return {'user': user, 'event_ts': event_ts}

            if event_type in ['user_check_in', 'user_check_out']:
                event_ts = Event.r1pwh(kwargs.get('event_id')).fetch1('event_ts')
                return {'user': user, 'event_ts': event_ts, 'check_in_out': 1 if event_type == 'user_check_in' else 0}

            if event_type == 'user_info_add':
                info_type = data.get('info_type')
                info = data.get('info')

                if info_type == 'slack_username':
                    info = slack_client.get_slack_username(info)
                    assert info is not None, 'Slack usename not found.'
                    
                return {'user': user, 'info_type': info_type, 'info': info}


@schema
class ProcessedEvent(djp.Lookup):
    hash_name = 'processed_id'
    definition = """
    processed_id : varchar(12) # 
    """

    @staticmethod
    def make(self, key):
        key['processed_id'] = self.hash1(key)
        try:
            data = Event.r1p(key).fetch1('data')
            key['pdata'] = EventHandler.run(data, **key)
            self.insert1(key, skip_hashing=True, insert_to_master=True)
        except Exception as e:
            key['error_msg'] = str(e)
            self.master.Attempted.insert1(key, insert_to_master=True, ignore_extra_fields=True)
            self.master.Log('error', f'Could not process event_id {key.get("event_id")}. Error msg: {e}')

    class Attempted(djp.Part):
        hash_name = 'processed_id'
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        error_msg : longblob #
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

    class TestEvent(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.TestEvent * EventHandler.TestEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)

    class UserAdd(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.UserAdd * EventHandler.UserEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)

    class UserInfoAdd(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.UserInfoAdd * EventHandler.UserEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)


    class UserAccess(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.UserAccess * EventHandler.UserEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)

    
    class UserCheckIn(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.UserCheckIn * EventHandler.UserEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)

    
    class UserCheckOut(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'processed_id'
        hashed_attrs = Event.primary_key + EventHandler.primary_key
        definition = """
        -> master
        -> Event
        -> EventHandler
        ---
        pdata : longblob # processed data
        processed_ts=CURRENT_TIMESTAMP : timestamp # timestamp entry inserted to this table
        index(event_id)
        """

        @classproperty
        def key_source(cls):
            restr = Event.UserCheckOut * EventHandler.UserEvent
            return (Event & restr) * (EventHandler & restr)

        def make(self, key):
            self.master.make(self, key)


@schema
class User(djp.Lookup):
    definition = """
    user : varchar(128) # username
    """

    class Username(djp.Part, dj.Computed):
        definition = """
        -> master
        -> Event
        -> ProcessedEvent
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        @classproperty
        def key_source(cls):
            return ProcessedEvent & ProcessedEvent.UserAdd

        def make(self, key):
            event_id, pdata = ProcessedEvent.r1p(key).fetch1('event_id', 'pdata')
            key.update({
                'event_id': event_id, 
                'user': pdata.get('user')
            })
            self.insert1(key, insert_to_master=True, ignore_extra_fields=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})

    class Info(djp.Part, dj.Computed):
        definition = """
        -> master
        -> Event
        -> ProcessedEvent
        ---
        info_type : varchar(250) #
        info : varchar(450)
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        @classproperty
        def key_source(cls):
            return ProcessedEvent & (ProcessedEvent.UserInfoAdd)

        def make(self, key):
            event_id, pdata = ProcessedEvent.r1p(key).fetch1('event_id', 'pdata')
            key.update({
                'event_id': event_id,
                'user': pdata.get('user'), 
                'info_type': pdata.get('info_type'), 
                'info': pdata.get('info')
            })
            self.insert1(key, ignore_extra_fields=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})

    class Accessed(djp.Part, dj.Computed):
        definition = """
        -> master
        -> Event
        -> ProcessedEvent
        ---
        ts_accessed : timestamp # timestamp user accessed
        ts_inserted : varchar(48)
        """

        @classproperty
        def key_source(cls):
            return ProcessedEvent & ProcessedEvent.UserAccess

        def make(self, key):
            event_id, pdata = ProcessedEvent.r1p(key).fetch1('event_id', 'pdata')
            key.update({
                'event_id': event_id, 
                'user': pdata.get('user'),
                'ts_accessed': pdata.get('event_ts'),
                'ts_inserted': current_timestamp('US/Central', fmt="%Y-%m-%d_%H:%M:%S.%f")
            })
            self.insert1(key, insert_to_master=True, ignore_extra_fields=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})

    class CheckInOut(djp.Part, dj.Computed):
        definition = """
        -> master
        -> Event
        -> ProcessedEvent
        ---
        check_in_out : tinyint # 1 if check-in, 0 if check-out
        ts_check_in_out : timestamp # timestamp user accessed
        ts_inserted : varchar(48)
        """

        @classproperty
        def key_source(cls):
            return (ProcessedEvent & ProcessedEvent.UserCheckIn) + (ProcessedEvent & ProcessedEvent.UserCheckOut)

        def make(self, key):
            event_id, pdata = ProcessedEvent.r1p(key).fetch1('event_id', 'pdata')
            key.update({
                'event_id': event_id, 
                'user': pdata.get('user'),
                'check_in_out': pdata.get('check_in_out'),
                'ts_accessed': pdata.get('event_ts'),
                'ts_inserted': current_timestamp('US/Central', fmt="%Y-%m-%d_%H:%M:%S.%f")
            })
            self.insert1(key, insert_to_master=True, ignore_extra_fields=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})


schema.spawn_missing_classes()
schema.connection.dependencies.load()
