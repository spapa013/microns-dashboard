import json
import logging
import time

import wridgets.app as wra
from ipywidgets import link
import datajoint_plus as djp
import pandas as pd
from microns_utils.misc_utils import wrap
from ..utils import GetDashboardUser, get_user_info_js
from ..schemas import dashboard as db

class UserApp(wra.App):
    store_config = [
        'user_app',
    ]
    
    get_user_info_js = get_user_info_js

    def make(self, **kwargs):
        self.propagate = True
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
        key = {'user': self.user}
        if len(db.User & key) == 0:
            try:
                    logging.info(f'user {key.get("user")} not found. Adding...')
                    event = db.Event.log_event('user_add', key)
            except:
                logging.exception('Could not add user to dashboard.Event.UserAdd')
        try:
            event = db.Event.log_event('user_access', key)
        except:
            logging.exception('Could not update dashboard.Event.UserAccess')
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
        'source',
        'all_protocols',
        'active_protocols',
        'protocol_id',
        'protocol_name'
    ]
    def make(self, source, on_select=None, manage=False, **kwargs):
        self.source = source
        self.propagate = True
        self.set_protocol_options()
        
        self._header_kws = dict(
            text="Protocol", 
            name='ProtocolSelectLabel'
        )
        self._toggle_buttons_kws = dict(
            wridget_type='ToggleButtons', 
            options=self.protocol_options,
            name='ProtocolSelectButtons',
            on_interact=self.set_protocol
        )
        self._select_button_kws = dict(
            description='Select', 
            on_interact=on_select,
            button_style='info',
            name='ProtocolSelectButton'
        )
        self._manage_button_kws = dict(
            description='Manage',
            on_interact=self.manage_protocols,
            button_style='warning',
            hide=not manage,
            name='ProtocolManageButton'
        )
        self._tags_kws = dict(
            value=self.active_protocols,
            allowed_tags=self.all_protocols,
            hide=True,
            name='ProtocolTags'
        )
        self.core = (
            (
                wra.Label(**self._header_kws) + \
                wra.SelectButtons(**self._toggle_buttons_kws) + \
                wra.ToggleButton(**self._select_button_kws) + \
                wra.ToggleButton(**self._manage_button_kws) 
            ) - \
            wra.Tags(**self._tags_kws)
        )
        self.set_protocol()
    
    def set_protocol(self):
        self.protocol_name = self.children.ProtocolSelectButtons.get1('label')
        self.protocol_id = self.children.ProtocolSelectButtons.get1('value')
    
    def set_protocol_options(self):
        ids, names, active = self.source.fetch('protocol_id', 'protocol_name', 'active', order_by='-ordering DESC')
        self.active_protocol_ids = ids[active.astype(bool)].tolist()
        self.active_protocols = names[active.astype(bool)].tolist()
        self.all_protocols = names.tolist()
    
    @property
    def protocol_options(self):
        if self.active_protocols is not None and self.active_protocol_ids is not None:
            return [(n, i) for n, i in zip(self.active_protocols, self.active_protocol_ids)]
    
    def manage_protocols(self):
        if self.children.ProtocolManageButton.get1('value'):
            self.children.ProtocolManageButton.set(description='Set')
            self.children.ProtocolTags.set(hide=False)
        else:
            self.update_protocols()
            self.set_protocol_options()
            self.children.ProtocolSelectButtons.set(options=self.protocol_options, value=self.protocol_options[0])
            self.children.ProtocolManageButton.set(description='Manage')
            self.children.ProtocolTags.set(hide=True)
    
    def update_protocols(self):
        updated = self.children.ProtocolTags.get1('value')
        for key in self.source:
            protocol_name = key.get('protocol_name')
            if protocol_name in updated:
                key.update({'active': 1})
                key.update({'ordering': updated.index(protocol_name)})
            else:
                key.update({'active': 0})
                key.update({'ordering': None})
            self.source.insert1(key, replace=True)   


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