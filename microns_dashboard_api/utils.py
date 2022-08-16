from traitlets import Unicode, Dict, Unicode
from ipywidgets import DOMWidget, register
import wridgets.app as wra
from ipywidgets import link

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
        'user'
    ]
    def make(self, globals_dict=None, on_user_update=None, **kwargs):
        self.globals_dict = {} if globals_dict is None else globals_dict
        self.user = self.globals_dict.get('user')
        self.on_user_update = self._on_user_update if on_user_update is None else on_user_update
        self.propagate = True
        
        self.core = (
            wra.Label(text='User', name='UserLabel') + \
            wra.Field(disabled=True, name='UserField', on_interact=self.on_user_update)
        )
        link((self.children.UserField.wridget.widget, 'value'), (self.user, 'name'))
    
    @property
    def user(self):
        return self.children.UserField.get1('value')
    
    def _on_user_update(self):
        pass
