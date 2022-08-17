import json
import time

import wridgets.app as wra
from ipywidgets import link
import datajoint_plus as djp
from ..utils import DashboardUser, get_user_info_js


class UserApp(wra.App):
    store_config = [
        'user_app'
    ]
    
    get_user_info_js = get_user_info_js

    def make(self, **kwargs):
        self.propagate = True
        self.on_user_update = self._on_user_update if kwargs.get('on_user_update') is None else kwargs.get('on_user_update')
        self.on_user_update_kwargs = {} if kwargs.get('on_user_update_kwargs') is None else kwargs.get('on_user_update_kwargs')
        self.core = (
            (
                wra.Label(text='User', name='UserLabel') + \
                wra.Field(disabled=True, name='UserField', on_interact=self.on_user_update)
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
            self.children.UserField.set(value=kwargs.get('user_info').get('name'))
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
                self.set(layout={'display': 'none'})

            if self.disable_on_login:
                self.set(disabled=True)
            
            self.on_login(**self.on_login_kwargs)
