if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def check_lpep_pickup_date(data, *args, **kwargs):
    return data[data.lpep_pickup_date == '2009-01-01']
