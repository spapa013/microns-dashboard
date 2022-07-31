"""
Externals for DataJoint tables.
"""

from pathlib import Path

import datajoint_plus as djp

base_path = Path() / '/mnt' / 'dj-stor01' / 'microns'
dashboard_path = base_path / 'dashboard'

dashboard = {
    'dashboard': djp.make_store_dict(dashboard_path)
}
