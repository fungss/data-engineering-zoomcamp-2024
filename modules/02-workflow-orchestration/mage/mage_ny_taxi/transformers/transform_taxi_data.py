import re


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def pascal_to_snake(pascal_string):
    # Insert underscores before uppercase letters (except before consecutive uppercase letters)
    snake_case = re.sub(r'(?<=[a-z0-9])([A-Z])|([A-Z])(?=[a-z0-9])', r'_\1\2', pascal_string)
    # Remove leading underscore and convert to lowercase
    snake_case = snake_case.lstrip('_').lower()
    return snake_case


@transformer
def transform(data, *args, **kwargs):
    """
    Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero
    """
    print("Preprocessing - remove {} rows with zero passengers or with zero trip distance".format(((data["passenger_count"] == 0) | (data['trip_distance'] == 0)).sum()))
    data = data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)]
    print("Preprocessing - create lpep_pickup_date column from lpep_pickup_datetime")
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    print("Preprocessing - rename columns in PascalCase to snake_case")
    print("Before: {}".format(data.columns))
    data.columns = [pascal_to_snake(col) for col in data.columns]
    print("After: {}".format(data.columns))
    print("Preprocessing - complete")

    print("Answers to homework questions...")
    print(f"Existing values of vendor_id are: {data.vendor_id.unique()}")
    return data


@test
def test_output(output, *args) -> None:
    assert "vendor_id" in output.columns, 'Column vendor_id does not exist'
    assert all((output["passenger_count"] > 0)) == True, 'There are rides with zero passenger_count'
    assert all((output["trip_distance"] > 0)) == True, 'There are rides with zero trip_distance'
