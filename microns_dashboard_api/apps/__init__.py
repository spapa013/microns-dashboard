import json
import logging
import time

import wridgets.app as wra
from ipywidgets import link
import numpy as np
import datajoint_plus as djp
import pandas as pd
from microns_utils.misc_utils import wrap
from ..utils import GetDashboardUser, get_user_info_js
from ..schemas import dashboard as db
from collections import namedtuple
logger = djp.getLogger(__name__)


class DataType:   
    Protocol = namedtuple('Protocol', ['ID', 'name', 'tag', 'active', 'ordering'])
    Protocol.__repr__ = lambda self: 'Protocol(' + ', '.join([f'{f}={getattr(self, f)}' for f in self._fields ]) + ')'


class AppLink(wra.App):
    def make(self, app1, app2, attr1='value', attr2='value', fwd_transform=None, rev_transform=None, orientation='vertical', **kwargs):
        self.setdefault('app1', app1)
        self.setdefault('app2', app2)
        self.setdefault('attr1', attr1)
        self.setdefault('attr2', attr2)
        self.setdefault('fwd_transform', fwd_transform)
        self.setdefault('rev_transform', rev_transform)
        self.setdefault('orientation', orientation)
        self.defaults.update(kwargs)
        
        app1_kws = self.setdefault('app1_kws', {})
        app2_kws = self.setdefault('app2_kws', {})
        
        # WrApps
        self._app1 = getattr(wra, self.getdefault('app1'))(**app1_kws)
        self._app2 = getattr(wra, self.getdefault('app2'))(**app2_kws)
        
        if self.getdefault('orientation') == 'vertical':
            self.core = self._app1 - self._app2
        elif self.getdefault('orientation') == 'horizontal':
            self.core = self._app1 + self._app2
        else:
            raise AttributeError('orientation not recognized. options are "vertical" or "horizontal"')
        
        fwd_transform = self.getdefault('fwd_transform')
        rev_transform = self.getdefault('rev_transform')
        transform = [fwd_transform, rev_transform] if fwd_transform is not None and rev_transform is not None else None
        link((self._app1.wridget.widget, self.getdefault('attr1')), ((self._app2.wridget.widget, self.getdefault('attr2'))), transform=transform)


class UserApp(wra.App):
    store_config = [
        'user_app',
        'user_info',
        'entry_point',
        'on_user_update',
        'on_user_update_kwargs',
    ]
    
    get_user_info_js = get_user_info_js

    def make(self, **kwargs):
        self.propagate = True
        self.entry_point = kwargs.get('entry_point')
        self.on_user_update = self.on_user_update if kwargs.get('on_user_update') is None else kwargs.get('on_user_update')
        self.on_user_update_kwargs = {} if kwargs.get('on_user_update_kwargs') is None else kwargs.get('on_user_update_kwargs')
        self.core = (
            (
                wra.Label(text='User', name='UserLabel') + \
                wra.Field(disabled=True, name='UserField', on_interact=self._on_user_update)
            ) - \
            (
                wra.Label(text='User Info', name='UserInfoLabel') + \
                wra.Field(disabled=True, name='UserInfoField', value='{}', wridget_type='Textarea', layout={'width': 'initial'})
            )
            
        )
        
        if 'user_app' in kwargs:
            self.user_app = kwargs.get('user_app')
            link((self.children.UserField.wridget.widget, 'value'), (self.user_app, 'name'))
            link((self.children.UserInfoField.wridget.widget, 'value'), (self.user_app, 'value'), transform=[json.loads, json.dumps])
            
        elif 'user_info' in kwargs:
            self.user_info = kwargs.get('user_info')
            self.children.UserField.set(value=kwargs.get('user_info').get('user'))
            self.children.UserInfoField.set(value=json.dumps(kwargs.get('user_info')))
    
    @property
    def user(self):
        return self.children.UserField.get1('value')
    
    @property
    def user_info(self):
        return json.loads(self.children.UserInfoField.get1('value'))
    
    def on_user_update(self, **kwargs):
        pass

    def _on_user_update(self):
        self.on_user_update(**self.on_user_update_kwargs)


class DataJointLoginApp(wra.App):
    store_config = [
        'is_connected'
    ]
    
    def make(self, **kwargs):
        self.hide_on_login = kwargs.get('hide_on_login')
        self.disable_on_login = kwargs.get('disable_on_login')
        self.on_login = self.on_login if kwargs.get('on_login') is None else kwargs.get('on_login')
        self.on_login_kwargs = {} if kwargs.get('on_login_kwargs') is None else kwargs.get('on_login_kwargs')
        self._header = wra.Label(text='DataJoint', fontsize=2)
        self._username_label = wra.Label(text='Username', name='UsernameLabel')
        self._username_field = wra.Field(name='UsernameField')
        self._password_label = wra.Label(text='Password', name='PasswordLabel')
        self._password_field = wra.Field(wridget_type='Password', name='PasswordField')
        self._login_button = wra.Button(description='Login', button_style='info', name='LoginButton', layout={'width': 'initial'}, on_interact=self._on_login) 
        
        self.core = (
            (self._header + self._login_button) - \
            ((
                self._username_label - self._password_label
            ) + \
            (
                self._username_field - self._password_field
            )
            )
        )
        
    def check_connection(self):
        try:
            djp.conn.connection
            self.is_connected = True
            self.msg('Connection established.')
            time.sleep(2)
        except:
            self.is_connected = False
            self.msg('Connection not established.')
            time.sleep(2)
    
    def on_login(self, **kwargs):
        pass
    
    def _on_login(self):
        if self._username_field.get1('value') != '':
            import logging
            logging.disable(50)
            djp.config['database.user'] = self._username_field.get1('value')
            djp.config['database.password'] = self._password_field.get1('value')
            logging.disable(logging.NOTSET)
            djp.conn(reset=True)
            self.check_connection()
            self.clear_output()

            if self.hide_on_login:
                self.set(hide=True)

            if self.disable_on_login:
                self.set(disabled=True)
            
            self.on_login(**self.on_login_kwargs)


class ProtocolManager(wra.App):
    store_config = [
        ('protocol_is_set', False)
    ]

    def make(self, source, on_set_protocol=None, on_set_protocol_kws=None, manage=False, **kwargs):
        self.source = source
        self.on_set_protocol = self.setdefault('on_set_protocol', on_set_protocol if on_set_protocol is not None else self.on_set_protocol)
        self.on_set_protocol_kws = self.setdefault('on_set_protocol_kws', on_set_protocol_kws if on_set_protocol_kws is not None else {})
        self.manage = self.setdefault('manage', manage)
        self.defaults.update(kwargs)
        
        label_kws = self.setdefault('label_kws', {})
        label_kws.setdefault('prefix', self.name)
        label_kws.setdefault('text', 'Protocol')
        
        active_select_label_kws = self.setdefault('active_select_label_kws', {})
        active_select_label_kws.setdefault('prefix', self.name)
        active_select_label_kws.setdefault('text', 'Active Protocols')
        active_select_label_kws.setdefault('fontsize', '0.5')
        active_select_label_kws.setdefault('minimize', True)
        
        active_select_kws = self.setdefault('active_select_kws', {})
        active_select_kws.setdefault('prefix', self.name)
        
        set_protocol_button_kws = self.setdefault('set_protocol_button_kws', {})
        set_protocol_button_kws.setdefault('prefix', self.name)
        set_protocol_button_kws.setdefault('description', 'Set')
        set_protocol_button_kws.setdefault('button_style', 'info')
        set_protocol_button_kws.setdefault('on_interact', self._on_set_protocol)
        
        refresh_button_kws = self.setdefault('refresh_button_kws', {})
        refresh_button_kws.setdefault('prefix', self.name)
        refresh_button_kws.setdefault('description', 'Refresh')
        refresh_button_kws.setdefault('on_interact', self.refresh)
        refresh_button_kws.setdefault('button_style', 'warning')
        
        manage_button_kws = self.setdefault('manage_button_kws', {})
        manage_button_kws.setdefault('prefix', self.name)
        manage_button_kws.setdefault('description', 'Manage')
        manage_button_kws.setdefault('minimize', not self.getdefault('manage'))
        manage_button_kws.setdefault('button_style', 'warning')
        manage_button_kws.setdefault('on_interact', self.on_manage)
        
        inactive_select_label_kws = self.setdefault('inactive_select_label_kws', {})
        inactive_select_label_kws.setdefault('prefix', self.name)
        inactive_select_label_kws.setdefault('text', 'Inactive Protocols')
        inactive_select_label_kws.setdefault('fontsize', '0.5')
        inactive_select_label_kws.setdefault('minimize', True)
        
        inactive_select_kws = self.setdefault('inactive_select_kws', {})
        inactive_select_kws.setdefault('prefix', self.name)
        inactive_select_kws.setdefault('minimize', True)
        
        set_active_button_kws = self.setdefault('set_active_button_kws', {})
        set_active_button_kws.setdefault('prefix', self.name)
        set_active_button_kws.setdefault('description', 'Set Active')
        set_active_button_kws.setdefault('button_style', 'warning')
        set_active_button_kws.setdefault('minimize', True)
        set_active_button_kws.setdefault('on_interact', self.update_source)
        set_active_button_kws.setdefault('on_interact_kws', dict(set_active=True))
        
        set_inactive_button_kws = self.setdefault('set_inactive_button_kws', {})
        set_inactive_button_kws.setdefault('prefix', self.name)
        set_inactive_button_kws.setdefault('description', 'Set Inactive')
        set_inactive_button_kws.setdefault('minimize', True)
        set_inactive_button_kws.setdefault('button_style', 'warning')
        set_inactive_button_kws.setdefault('on_interact', self.update_source)
        set_inactive_button_kws.setdefault('on_interact_kws', dict(set_inactive=True))
        
        # Set WrApps
        self._label = wra.Label(**label_kws)
        self._active_select_label = wra.Label(**active_select_label_kws)
        self._active_select = wra.Select(options=self.active_protocol_options, **active_select_kws)
        self._set_protocol_button = wra.ToggleButton(**set_protocol_button_kws)
        self._refresh_button = wra.Button(**refresh_button_kws)
        self._manage_button = wra.ToggleButton(**manage_button_kws)
        self._inactive_select_label = wra.Label(**inactive_select_label_kws)
        self._inactive_select = wra.Select(options=self.inactive_protocol_options, **inactive_select_kws)
        self._set_active_button = wra.Button(**set_active_button_kws)
        self._set_inactive_button = wra.Button(**set_inactive_button_kws)
        
        # Set core
        self.core = (
            self._label - \
            (
                (
                    self._active_select_label - self._active_select - self._set_inactive_button
                ) + \
                (
                    self._inactive_select_label - self._inactive_select - self._set_active_button
                )
            ) - \
            (
                self._set_protocol_button + \
                self._refresh_button + \
                self._manage_button                
            )
        )

    def on_set_protocol(self):
        pass
    
    def _on_set_protocol(self, **kwargs):
        if self._set_protocol_button.get1('value'):
            self._set_protocol_button.set(description='Unset')
            self.on_set_protocol(**self.on_set_protocol_kws)
            self.set(disabled=True, exclude=self._set_protocol_button.name)
            self.protocol_is_set = True
        else:
            self._set_protocol_button.set(description='Set')
            self.set(disabled=False)
            self.protocol_is_set = False
    
    def on_manage(self):
        if self._manage_button.get1('value'):
            self._set_protocol_button.set(disabled=True)
            self._manage_button.set(description='Hide Manage Tools')
            self._active_select_label.minimize = False
            self._inactive_select.updatedefault('minimize', False)
            self._inactive_select_label.minimize = False
            self._inactive_select.minimize = False
            self._set_active_button.minimize = False
            self._set_inactive_button.minimize = False
        else:
            self._manage_button.set(description='Manage')
            self._active_select_label.minimize = True
            self._inactive_select_label.minimize = True
            self._inactive_select.updatedefault('minimize', True)
            self._inactive_select.minimize = True
            self._set_active_button.minimize = True
            self._set_inactive_button.minimize = True
            self._set_protocol_button.set(disabled=False)
    
    @property
    def protocols(self):
        return [
            DataType.Protocol(
                ID=row.get('protocol_id'), 
                name=row.get('protocol_name'), 
                tag=row.get('tag'), 
                active=row.get('active'), 
                ordering=row.get('ordering')
                ) for row in self.source.fetch(as_dict=True, order_by='-ordering DESC')
        ]
    
    @property
    def active_protocols(self):
        return [p for p in self.protocols if p.active==1]
    
    @property
    def inactive_protocols(self):
        return [p for p in self.protocols if p.active==0]
    
    def _format_protocol_object(self, protocol_obj:DataType.Protocol):
        return (f'{protocol_obj.name} ({protocol_obj.ID[:4]})', protocol_obj)
    
    @property
    def protocol_options(self):
        return [self._format_protocol_object(p) for p in self.protocols]
    
    @property
    def active_protocol_options(self):
        return [self._format_protocol_object(p) for p in self.active_protocols]
    
    @property
    def inactive_protocol_options(self):
        return [self._format_protocol_object(p) for p in self.inactive_protocols]
    
    def update_source(self, set_active=None, set_inactive=None):
        assert (set_active is None) ^ (set_inactive is None), 'either set_active or set_inactive must be True'
        
        def update(protocol_id):
            update_dict = (self.source & {'protocol_id': protocol_id}).fetch1()
            if set_active is not None:
                update_dict.update({'active': 1})
                orderings = [p.ordering for p in self.active_protocols if p.ordering is not None]
                if orderings:
                    last_idx = np.max(orderings)
                else:
                    last_idx = -1
                update_dict.update({'ordering': 1 + last_idx})
            if set_inactive is not None:
                update_dict.update({'active': 0})
                update_dict.pop('ordering')
            update_dict.pop('last_updated')
            self.source.insert1(update_dict, replace=True)
        
        if set_active is not None:
            protocol_id = self._inactive_select.get1('value').ID
        if set_inactive is not None:
            protocol_id = self._active_select.get1('value').ID
        
        update(protocol_id)        
        self.refresh()
        
    def refresh(self):
        self._active_select.updatedefault('options', self.active_protocol_options)
        self._inactive_select.updatedefault('options', self.inactive_protocol_options)
        self._active_select.reset()
        self._inactive_select.reset()


class DataJointTableApp(wra.App):
    store_config = [
        'source',
        'attrs',
        'n_rows'
    ]
    def make(self, source, attrs=None, n_rows=25, **kwargs):
        self.source = source
        self.attrs = source.heading.names if attrs is None else wrap(attrs)
        self.n_rows = n_rows

    def to_df(self, restrict=None, subtract=None):
        restr = {} if restrict is None else restrict
        subtr = [] if subtract is None else subtract

        df = pd.DataFrame(
            (
                (self.source & restr) - subtr
            ).fetch(limit=self.n_rows)
        )
        df = df[[*self.attrs]]
        return df


class UserInfoManager(wra.App):
    store_config = [
        'label',
        'get_data_kws',
        'set_data_kws'
    ]
    
    def make(self, label, get_data=None, set_data=None, get_data_kws=None, set_data_kws=None, **kwargs):
        self.label = label
        self.get_data = self.get_data if get_data is None else get_data
        self.set_data = self.set_data if set_data is None else set_data
        self.get_data_kws = {} if get_data_kws is None else get_data_kws
        self.set_data_kws = {} if set_data_kws is None else set_data_kws
        label_kws = dict(
            text=self.label
        )
        field_kws = dict(
            value=self._get_data(**self.get_data_kws),
            disabled=True
            
        )
        button_kws = dict(
            description='Update', 
            button_style='info',
            on_interact=self._set_data,
            on_interact_kws=self.set_data_kws
        )
        self.core = (
            wra.Label(**label_kws) + \
            wra.Field(**field_kws) + \
            wra.ToggleButton(**button_kws)
        )
    
    def get_data(self, **get_data_kws):
        pass
    
    def set_data(self, **set_data_kws):
        pass
    
    def _get_data(self, **get_data_kws):
        return self.get_data(**get_data_kws)

    def _set_data(self, **set_data_kws):
        if self.children.ToggleButton.get1('value'):
            self.children.Field.set(disabled=False)
            self.children.ToggleButton.set(description='Set')
        else:
            self.children.Field.set(disabled=True)
            self.children.ToggleButton.set(description='Update')
            self.set_data(self.children.Field.get1('value'), **set_data_kws)
