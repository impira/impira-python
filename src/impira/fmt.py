from datetime import datetime
from pydantic import validate_arguments


def american_date(d: datetime.date):
    return d.strftime("%m/%d/%Y")
