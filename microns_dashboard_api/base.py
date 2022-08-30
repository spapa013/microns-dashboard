"""
Base classes and methods for microns-dashboard
"""
from collections import namedtuple
from functools import wraps
import inspect
import json
from pathlib import Path
from time import timezone
import datajoint as dj
import datajoint_plus as djp
from datajoint_plus.utils import format_rows_to_df
from datajoint_plus import base as djpb
from datajoint_plus import user_tables as djpu
from microns_utils.misc_utils import classproperty, wrap
from microns_utils.version_utils import check_package_version
from microns_utils.datetime_utils import current_timestamp


Version = namedtuple('Version', ['id', 'version', 'timestamp'])
EventData = namedtuple('EventData', ['id', 'name', 'timestamp'])

version_sqltype = "varchar(128)"
version_id_sqltype = "varchar(6)"
event_id_sqltype = "varchar(12)"
event_sqltype = "varchar(128)"
event_handler_id_sqltype = "varchar(6)"

class Base:
    @classproperty
    def definition(cls):
        return "\n".join([l.strip() for l in [cls.default_primary_attrs, cls.extra_primary_attrs, """---""", cls.default_secondary_attrs, cls.extra_secondary_attrs]]) 
    

class VersionLookup(Base, djpb.BaseMaster, djpu.UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        assert hasattr(cls, 'package'), 'subclasses of VersionLookup must implement "package"'
    
    enable_hashing = True
    attr_name = 'version'

    @classproperty
    def hash_name(cls):
        return '_'.join([cls.attr_name, 'id'])

    @classproperty
    def hashed_attrs(cls):
        return cls.attr_name
    
    @classproperty
    def default_primary_attrs(cls):
        return f"""
        {cls.attr_name} : {version_sqltype} # package version
        """
    
    @classproperty
    def extra_primary_attrs(cls):
        return """"""
    
    @classproperty
    def default_secondary_attrs(cls):
        return f"""
        {cls.hash_name} : {version_id_sqltype} # hash of package version
        timestamp=CURRENT_TIMESTAMP : timestamp # timestamp when version was inserted
        """

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""
    
    @classproperty
    def version(cls):
        return check_package_version(package=cls.package)

    @classproperty
    def version_id(cls):
        return cls.hash1({cls.attr_name: cls.version})

    @classproperty
    def contents(cls):
        cls.insert1({cls.attr_name: cls.version}, ignore_extra_fields=True, skip_duplicates=True)
        return {}


class EventLookup(Base, djpb.BaseMaster, djpu.UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    hash_name = 'event_id'

    @classmethod
    def get(cls, key):
        return cls.r1p(key).fetch()

    @classmethod
    def get1(cls, key):
        return cls.r1p(key).fetch1()

    @classproperty
    def default_primary_attrs(cls):
        return f"""
        {cls.hash_name} : {event_id_sqltype}
        """

    @classproperty
    def extra_primary_attrs(cls):
        return """"""

    @classproperty
    def default_secondary_attrs(cls):
        return f"""
        event=NULL : {event_sqltype}
        timestamp : timestamp # 
        """

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""

    @classmethod
    def log_event(cls, event, attrs=None, data=None):
        parts = [part for part in cls.parts(as_cls=True) if event in getattr(part, 'events')]
        assert len(parts) >= 1, f'No parts with event "{event}" found.'
        assert len(parts) < 2, f'Multiple parts with event "{event}" found. Parts are: {[p.class_name for p in parts]}'
        return parts[0].log_event(event, attrs=attrs, data=data)

    @classmethod
    def events(cls):
        events = []
        for part in cls.parts(as_cls=True):
            if issubclass(part, Event):
                events.extend(wrap(part.events))
        return events


class Event(Base, djpb.BasePart, djpu.UserTable, dj.Part):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        assert getattr(cls, 'events', None) is not None, 'Subclasses of Event must implement "events".'

    enable_hashing = True
    hash_name = 'event_id'
    hashed_attrs = 'event', 'timestamp'
    events = None
    data_type = 'longblob'
    required_keys = None
    external_type = None
    basedir = None
    file_type = None
    constant_attrs = None

    @classproperty
    def default_primary_attrs(cls):
        return f"""
        -> master
        event : {event_sqltype}
        timestamp : timestamp # event timestamp
        """

    @classproperty
    def extra_primary_attrs(cls):
        return """"""

    @classproperty
    def default_secondary_attrs(cls):
        return f"""data=NULL : {cls.data_type} # event associated data. default=NULL"""

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""

    @classmethod
    def prepare_data(cls, event, data=None):        
        if cls.external_type is not None:
            if cls.external_type == 'filepath':
                required = ['basedir', 'file_type']
                assert getattr(cls, required) is not None, f'Subclasses of Event must implement "{required}" if external_type == "filepath".'
                basedir = Path(cls.basedir)
                basedir.mkdir(exist_ok=True)
                filename = basedir.joinpath(event.id).with_suffix(cls.file_type)

                try:
                    if cls.file_type == '.json':
                        with open(filename, "w") as f:
                            f.write(json.dumps(data))
                        return filename
                    else:
                        raise NotImplementedError(f'file_type "{cls.file_type}" not currently supported.')
                except:
                    raise Exception(f'Unable to create {cls.file_type} file.')
            else:
                raise NotImplementedError(f'external_type "{cls.external_type}" not currently supported.')
        else:
            return data
    
    @classmethod
    def log_event(cls, event, attrs=None, data=None):
        assert event in cls.events, f'event not found. events: {cls.events}'
        timestamp = current_timestamp('US/Central', fmt="%Y-%m-%d_%H:%M:%S.%f")
        event_id = cls.hash1({'event': event, 'timestamp': timestamp})
        event = EventData(id=event_id, name=event, timestamp=timestamp)
        row = {'event_id': event.id, 'event': event.name, 'timestamp': event.timestamp}
        row.update({} if attrs is None else attrs)
        row['data'] = cls().prepare_data(event=event, data=data)
        cls.insert1(row=row, constant_attrs={} if cls.constant_attrs is None else cls.constant_attrs, insert_to_master=True, ignore_extra_fields=True, skip_hashing=True)
        cls.master.Log('info',  f'Event "{event.name}" with event_id {event.id} occured at {event.timestamp}')
        cls.master.Log('debug', f'Event "{event.name}" with event_id {event.id} occured at {event.timestamp} with insert {row}')
        cls().on_event(event=event)
        return event

    @classmethod
    def on_event(cls, event):
        pass


class EventHandlerLookup(Base, djpb.BaseMaster, djpu.UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    hash_name = 'event_handler_id'

    @classproperty
    def default_primary_attrs(cls):
        return f"""
        {cls.hash_name} : {event_handler_id_sqltype} # id of event handler
        """

    @classproperty
    def extra_primary_attrs(cls):
        return """"""

    @classproperty
    def default_secondary_attrs(cls):
        return f"""
        timestamp=CURRENT_TIMESTAMP : timestamp
        """

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""


class EventHandler(Base, djpb.BasePart, djpu.UserTable, dj.Part, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    enable_hashing = True
    hash_name = 'event_handler_id'
    hashed_attrs = 'event', 'version'

    @classproperty
    def default_primary_attrs(cls):
        return f"""
        -> master
        event : {event_sqltype}
        """

    @classproperty
    def extra_primary_attrs(cls):
        return """"""

    @classproperty
    def default_secondary_attrs(cls):
        return f""""""

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""


class Maker(Base, djpb.BasePart, djpu.UserTable, dj.Part, dj.Computed):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
    
    enable_hashing = True
    
    @classproperty
    def hash_name(cls):
        return cls.hash_name
    
    @classproperty
    def hashed_attrs(cls):
        return cls.upstream.primary_key + cls.method.primary_key
    
    @classproperty
    def default_primary_attrs(cls):
        return f"""
        -> master
        -> {cls.upstream.class_name}
        -> {cls.method.class_name}
        """

    @classproperty
    def extra_primary_attrs(cls):
        return """"""
    
    @classproperty
    def default_secondary_attrs(cls):
        return f""""""

    @classproperty
    def extra_secondary_attrs(cls):
        return """"""

    def make(self, key):
        key[self.hash_name] = self.hash1(key)
        key.update(self.upstream.get1(key))
        key.update(self.method.run(key))
        self.insert1(key, ignore_extra_fields=True, insert_to_master=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True}, skip_hashing=True)
        self.on_make(key)

    def on_make(self, key):
        pass