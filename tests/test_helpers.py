import pytest


from aiida_jutools.sisc_lab import helpers
from aiida_jutools.sisc_lab.helpers import print_bold, get_structure_workflow_dict, generate_dict_property_pandas_source
from aiida_jutools.sisc_lab.helpers import generate_structure_property_pandas_source, generate_combination_property_pandas_source, read_json_file
from aiida_jutools.sisc_lab.helpers import bokeh_struc_prop_vis, print_Count, get_data_node_count, get_process_node_count, draw_pie_chart
# Write here tests for helper functions
# exmaple


def test_read_json_file():
    """Tests the function read json file"""

    assert True
