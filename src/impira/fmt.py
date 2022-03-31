from datetime import datetime
from pydantic import validate_arguments


def unambiguous_date(d: datetime.date):
    return d.strftime("%Y-%m-%d")

def american_date(d: datetime.date):
    return d.strftime("%m/%d/%Y")
