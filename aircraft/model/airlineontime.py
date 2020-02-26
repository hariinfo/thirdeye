"""
Object representation of the cassandra table
"""
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

class airlineontime(Model):
#  __keyspace__ = 'thirdeye_test'
  __table_name__ = 'airlineontime'
  id = columns.UUID(primary_key=True)
  crsdeptime = columns.Integer()
  crsarrtime = columns.Integer()
  flight_date = columns.Date()
  reporting_airline = columns.Text()
  tail_number = columns.Text()
  originairportid = columns.Integer()
  destairportid = columns.Integer()
  carriername =  columns.Text()
  manufacturer =  columns.Text()
  aircraft_issue_date = columns.Date()
  aircraft_model = columns.Text()
  aircraft_type = columns.Text()
  aircraft_engine = columns.Text()
  origincityname = columns.Text()
  depdel15 = columns.Integer()
  arrdel15 = columns.Integer()
  carrierdelay = columns.Integer()
  weatherdelay = columns.Integer()
  nasdelay = columns.Integer()
  securitydelay = columns.Integer()
  lateaircraftdelay = columns.Integer()
  close = columns.Decimal()