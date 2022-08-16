import wridgets.app as wra
from ipywidgets import link
import json


class UserApp(wra.App):
    store_config = [
        'user',
        'user_info',
        'user_app'
    ]
    
    def make(self, **kwargs):
        self.propagate = True
        
        self.core = (
            wra.Label(text='User', name='UserLabel') + \
            wra.Field(disabled=True, name='UserField', on_interact=self.on_user_field_update) + \
            wra.Field(disabled=True, name='UserInfoField', layout={'display': 'none'}, value='{}', wridget_type='Textarea', on_interact=self.on_user_info_field_update)
        )
        
        if 'user_app' in kwargs:
            self.user_app = kwargs.get('user_app')
            link((self.children.UserField.wridget.widget, 'value'), (self.user_app, 'name'))
            link((self.children.UserInfoField.wridget.widget, 'value'), (self.user_app, 'value'), transform=[json.loads, json.dumps])
            
        elif 'user_info' in kwargs:
            self.children.UserField.set(value=kwargs.get('user_info').get('name'))
            self.children.UserInfoField.set(value=json.dumps(kwargs.get('user_info')))

    def on_user_field_update(self):
        self.user = self.children.UserField.get1('value')
    
    def on_user_info_field_update(self):
        self.user_info = json.loads(self.children.UserInfoField.get1('value'))