import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    value_list = []
    for col in data.columns:
        if col.islower():
            value = col
        else:
            value = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', col).lower()
        value_list.append(value)
    data.columns = value_list
    print(data.columns)
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'].isin(output['vendor_id']).all(), "Assertion failed: vendor_id contains invalid values"

    # Assertion 2: passenger_count is greater than 0
    assert (output['passenger_count'] > 0).all(), "Assertion failed: passenger_count contains values less than or equal to 0"

    # Assertion 3: trip_distance is greater than 0
    assert (output['trip_distance'] > 0).all(), "Assertion failed: trip_distance contains values less than or equal to 0"
