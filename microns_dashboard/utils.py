import wridgets.app as wra
from traitlets import Unicode, Dict, Unicode
from ipywidgets import DOMWidget, register
from ipywidgets import link
import json

get_user_info = """
    require.undef('user_widget');

    define('user_widget', ["@jupyter-widgets/base"], function (widgets) {

        
        var UserView = widgets.DOMWidgetView.extend({
            initialize: function(attributes, options) {

            this.response = fetch(
                    '/hub/dashboards-api/hub-info/user',
                    { 
                    mode: 'no-cors', 
                    credentials: 'same-origin',
                    headers: new Headers({'Access-Control-Allow-Origin':'*'}) 
                });
                this.response.then( response => {
                    this.result = response.json();
                    this.result.then( json => {
                        this.model.set('value', json);
                        this.model.set('name', json.name);
                        this.model.save_changes();
                    });

                });
            
            },
            
            render: async function () {
                await this.response;
                await this.result;
                var json = this.model.get('value');
                var text = 'No user';
                if (json.hasOwnProperty('name')) {
                    text = '';
                }
                this.el.appendChild(document.createTextNode(text));

            },
        
        });

        return {
            UserView: UserView
        };
    });
    """


@register
class DashboardUser(DOMWidget):
    """
    Get JupyterHub user info
    https://gist.github.com/danlester/ac1d5f29358ce1950482f8e7d4301f86
    """
    _view_name = Unicode('UserView').tag(sync=True)
    _view_module = Unicode('user_widget').tag(sync=True)
    _view_module_version = Unicode('0.1.0').tag(sync=True)

    value = Dict({}, help="User info").tag(sync=True)
    name = Unicode('').tag(sync=True)


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