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

    
    @classproperty
    def default_primary_attrs(cls):
        return f"""
        event_id : {event_id_sqltype}
        """

    @classproperty
    def additional_primary_attrs(cls):
        return """
        """

    @classproperty
    def default_secondary_attrs(cls):
        return f"""
        event_type : {event_type_sqltype}
        event_ts : {event_ts_sqltype} # event timestamp
        """

    @classproperty
    def additional_secondary_attrs(cls):
        return """
        """

    @classproperty
    def definition(cls):
        return cls.default_primary_attrs + cls.additional_primary_attrs + """---""" + cls.default_secondary_attrs + cls.additional_secondary_attrs  

    @classmethod
    def log_event(cls, event_type, data=None):
        parts = [part for part in cls.parts(as_cls=True) if getattr(part, 'event_type') == event_type]
        assert len(parts) >= 1, f'No parts with event_type "{event_type}" found.'
        assert len(parts) < 2, f'Multiple parts with event_type "{event_type}" found. Parts are: {[p.class_name for p in parts]}'
        return parts[0].log_event(data)

    @classmethod
    def types(cls):
        event_types = [part.event_type for part in cls.parts(as_cls=True) if getattr(part, 'event_type', None) is not None]
        nt_type = 'EventTypes'
        nt = namedtuple(nt_type, field_names=event_types)
        nt.__repr__ = lambda self: f'{nt_type}(' + ', '.join(self._fields) + ')'
        return nt(*event_types)


class EventType(BasePart, UserTable, dj.Part):
    def __init_subclass__(cls, **kwargs):
        cls._init_validation(**kwargs)
        assert getattr(cls, 'event_type', None) is not None, 'Subclasses of EventType must implement "event_type".'

    event_type = None
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
        return f"""
        data=NULL : {cls.data_type} # event associated data. default=NULL
        """

    @classproperty
    def additional_secondary_attrs(cls):
        return """
        """

    @classproperty
    def definition(cls):
        return cls.default_primary_attrs + cls.additional_primary_attrs + """---""" + cls.default_secondary_attrs + cls.additional_secondary_attrs  

                        
    @classmethod
    def prepare_data(cls, event, data=None):        
        if cls.required_keys is not None:
            assert data is not None, 'data cannot be None if required_keys is not None'
            try:
                data_df = format_rows_to_df(data)
            except:
                raise AttributeError('Data could not be converted to a dataframe to cross-check columns against required_keys.')
                
            for key in wrap(cls.required_keys):
                assert key in data_df.columns, f'data must include "{key}"'
        
        row = {'data': data}
        
        if data is None or cls.external_type is None:
            return row
        
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
    
    @classmethod
    def log_event(cls, data=None):
        tz = 'US/Central'
        timestamp = current_timestamp(tz, fmt="%Y-%m-%d_%H:%M:%S.%f")
        event_id = cls.master.hash1({'event_type': cls.event_type, 'event_ts': timestamp})
        event = Event(id=event_id, type=cls.event_type, timestamp=timestamp, timezone=tz)
        constant_attrs = {'event_id': event.id, 'event_type': event.type, 'event_ts': event.timestamp}
        if cls.constant_attrs is not None:
            constant_attrs.update(cls.constant_attrs)
        row = cls().prepare_data(event, data)
        cls.master.insert1(row=row, constant_attrs=constant_attrs, insert_to_parts=cls, ignore_extra_fields=True, skip_hashing=True)
        cls.master.Log('info', f'Event with event_id {event.id} and event_type "{cls.event_type}" occured at {event.timestamp} {event.timezone}')
        cls.master.Log('debug', f'Event with event_id {event.id} and event_type "{cls.event_type}" occured at {event.timestamp} {event.timezone} with insert {row}')
        cls().on_event(event)
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


    