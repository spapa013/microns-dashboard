"""
Externals for DataJoint tables.
"""

from pathlib import Path

import datajoint_plus as djp

base_path = Path() / '/mnt' / 'dj-stor01' / 'microns' / 'dashboard'
dashboard_events_path = base_path / 'events'


dashboard = {
    'events': djp.make_store_dict(dashboard_events_path)
}
