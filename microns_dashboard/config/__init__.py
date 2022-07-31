"""
Configuration package/module for microns-coregistration.
"""
import datajoint_plus as djp
from microns_utils.config_utils import SchemaConfig
from . import adapters
from . import externals

djp.enable_datajoint_flags()

dashboard_config = SchemaConfig(
    module_name='dashboard',
    schema_name='microns_external_dashboard',
    externals=externals.dashboard,
    adapters=adapters.dashboard
)