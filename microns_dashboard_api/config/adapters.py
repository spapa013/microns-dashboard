"""
Adapters for DataJoint tables.
"""

from microns_utils.adapter_utils import JsonAdapter

events = JsonAdapter('filepath@events')

dashboard = {
    'events': events
}