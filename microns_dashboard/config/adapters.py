"""
Adapters for DataJoint tables.
"""

from microns_utils.adapter_utils import JsonAdapter

dashboard = {'dashboard_jsons': JsonAdapter('filepath@dashboard_jsons')}