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
from datajoint_plus.base import BaseMaster, BasePart
from datajoint_plus.user_tables import UserTable
from microns_utils.misc_utils import classproperty, wrap
from microns_utils.version_utils import check_package_version
from microns_utils.datetime_utils import current_timestamp


class VersionLookup(BaseMaster, UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        assert hasattr(cls, 'package'), 'subclasses of VersionLookup must implement "package"'
    enable_hashing = True
    hash_name = 'version_id'
    hashed_attrs = 'version'
    @classproperty
    def definition(cls):
        return f"""
                {cls.hash_name} : varchar(6) # hash of package version
                ---
                version : varchar(48) # package version
                timestamp=CURRENT_TIMESTAMP : timestamp # timestamp when version was inserted
                """
    
    @classproperty
    def current_version(cls):
        return check_package_version(package=cls.package)

    @classproperty
    def current_version_id(cls):
        return cls.hash1({'version': cls.current_version})

    @classproperty
    def contents(cls):
        cls.insert1({
            'version' : cls.current_version
        }, ignore_extra_fields=True, skip_duplicates=True)
        return {}


Event = namedtuple('Event', ['id', 'type', 'timestamp', 'timezone'])
event_id_sqltype = "varchar(12)"
event_type_sqltype = "varchar(450)"
event_ts_sqltype = 'timestamp'


class EventLookup(BaseMaster, UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    enable_hashing = True
    hash_name = 'event_id'
    hashed_attrs = 'event_type', 'event_ts'

    @classmethod
    def get(cls, key):
        return cls.r1p(key).fetch()

    @classmethod
    def get1(cls, key):
        return cls.r1p(key).fetch1()

    @classproperty
    def default_primary_attrs(cls):
        return f"""event_id : {event_id_sqltype} \n"""

    @classproperty
    def additional_primary_attrs(cls):
        return """\n"""

    @classproperty
    def default_secondary_attrs(cls):
        return f"""
        event_type : {event_type_sqltype}
        event_ts : {event_ts_sqltype} # event timestamp
        """

    @classproperty
    def additional_secondary_attrs(cls):
        return """\n"""

    @classproperty
    def definition(cls):
        return cls.default_primary_attrs + cls.additional_primary_attrs + """--- \n""" + cls.default_secondary_attrs + cls.additional_secondary_attrs  

    @classmethod
    def log_event(cls, event_type, attrs=None, data=None):
        parts = [part for part in cls.parts(as_cls=True) if event_type in getattr(part, 'event_types')]
        assert len(parts) >= 1, f'No parts with event_type "{event_type}" found.'
        assert len(parts) < 2, f'Multiple parts with event_type "{event_type}" found. Parts are: {[p.class_name for p in parts]}'
        return parts[0].log_event(event_type, attrs=attrs, data=data)

    @classmethod
    def event_types(cls):
        event_types = []
        for part in cls.parts(as_cls=True):
            if issubclass(part, EventType):
                event_types.extend(wrap(part.event_types))
        return event_types


class EventType(BasePart, UserTable, dj.Part):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        assert getattr(cls, 'event_types', None) is not None, 'Subclasses of EventType must implement "event_types".'

    event_types = None
    hash_name = 'event_id'
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
        event_type : {event_type_sqltype}
        event_ts : {event_ts_sqltype} # event timestamp
        """

    @classproperty
    def additional_primary_attrs(cls):
        return """
        """

    @classproperty
    def default_secondary_attrs(cls):
        return f"""data=NULL : {cls.data_type} # event associated data. default=NULL \n"""

    @classproperty
    def additional_secondary_attrs(cls):
        return """\n"""

    @classproperty
    def definition(cls):
        return cls.default_primary_attrs + cls.additional_primary_attrs + """--- \n""" + cls.additional_secondary_attrs + cls.default_secondary_attrs  
      
    @classmethod
    def prepare_data(cls, event, data=None):        
        if cls.external_type is not None:
            if cls.external_type == 'filepath':
                required = ['basedir', 'file_type']
                assert getattr(cls, required) is not None, f'Subclasses of EventType must implement "{required}" if external_type == "filepath".'
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
    def log_event(cls, event_type, attrs=None, data=None):
        assert event_type in cls.event_types, f'event_type not in allowed event_types. event_types: {cls.event_types}'
        tz = 'US/Central'
        timestamp = current_timestamp(tz, fmt="%Y-%m-%d_%H:%M:%S.%f")
        event_id = cls.master.hash1({'event_type': event_type, 'event_ts': timestamp})
        event = Event(id=event_id, type=event_type, timestamp=timestamp, timezone=tz)
        row = {'event_id': event.id, 'event_type': event.type, 'event_ts': event.timestamp}
        row.update({} if attrs is None else attrs)
        row['data'] = cls().prepare_data(event=event, data=data)
        cls.master.insert1(row=row, constant_attrs={} if cls.constant_attrs is None else cls.constant_attrs, insert_to_parts=cls, ignore_extra_fields=True, skip_hashing=True)
        cls.master.Log('info', f'Event with event_id {event.id} and event_type "{event.type}" occured at {event.timestamp} {event.timezone}')
        cls.master.Log('debug', f'Event with event_id {event.id} and event_type "{event.type}" occured at {event.timestamp} {event.timezone} with insert {row}')
        cls().on_event(event=event)
        return event

    @classmethod
    def on_event(cls, event):
        pass

    def drop(self, force=False):
        if force:
            super(UserTable, self).drop()
        else:
            raise dj.DataJointError('Cannot drop a Part directly.  Delete from master instead')

    def drop_quick(self, force=False):
        if force:
            return super(UserTable, self).drop_quick()
        else:
            raise dj.DataJointError('Cannot drop a Part directly.  Delete from master instead')


class EventHandlerLookup(BaseMaster, UserTable, dj.Lookup):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)

    hash_name = 'event_handler_id'
    definition = """
    event_handler_id : varchar(6) # id of event handler
    """
    current_version_id = ''

    @classmethod
    def run(cls, key):
        handler = cls.r1p(key)
        handler_version_id = handler.fetch1('version_id')
        cls.Log('info', 'Running %s', handler.class_name)
        cls.Log('debug', 'Running %s with key %s', handler.class_name, key)
        assert  handler_version_id == cls.current_version_id, f'Version mismatch, event_handler version_id is {handler_version_id} but the current version_id is {cls.current_version_id}'
        key = handler.run(key)
        cls.Log('info', '%s ran successfully.', handler.class_name)
        return key