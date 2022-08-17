import wridgets.app as wra
from ipywidgets import link
import json
from ..utils import get_user_info_js, DashboardUser

class UserApp(wra.App):
    store_config = [
        'user_app'
    ]
    
    get_user_info_js = get_user_info_js
    show_user_info_app = DashboardUser

    def make(self, **kwargs):
        self.propagate = True
        self.on_user_update = self._on_user_update if kwargs.get('on_user_update') is None else kwargs.get('on_user_update')
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
    
    def _on_user_update(self):
        pass
