from streamsx.topology.topology import Topology
from streamsx.topology import schema
from streamsx.topology.context import *
from streams.observations import observation_stream  # Cannot use "as X"

import hashlib

from streamsx_health.ingest.Observation import getReadingValue

service_name = "Streaming Analytics-no"  # Service name
credentials = {}  # Put redentials here

def anonymize(tuple_):
    tuple_['patientId'] = hashlib.sha256(tuple_['patientId'].encode('utf - 8')).digest()
    tuple_['device']['locationId'] = hashlib.sha256(tuple_['device']['locationId'].encode('utf - 8')).digest()

    return tuple_

class Avg(object):

    def __init__(self, max_):
        self.history_max = max_  # The maximum length of the array we will use to calculate the average
        self.last_n = []

    # Object is callable
    def __call__(self, tuple_):
        self.last_n.append(getReadingValue(tuple_))
        if len(self.last_n) > self.history_max:
            self.last_n.pop(0)

        return sum(self.last_n) / len(self.last_n)


# Set up access to Streaming Analytics service
vs = {'streaming-analytics': [{'name': service_name, 'credentials': credentials}]}
cfg = {
    ConfigParams.SERVICE_NAME: service_name,
    ConfigParams.VCAP_SERVICES: vs
}

# Define data source

# Create Topology and read from data source
topo = Topology()
patient_data_stream = topo.subscribe('ingest-beacon', schema.CommonSchema.Json)

# Anonymize patient
anonymous_patient_stream = patient_data_stream.map(anonymize)

# Filtering data
heart_rate_stream = anonymous_patient_stream.filter(lambda tuple_: (tuple_['reading']['readingType']['code'] == '8867-4'))

# Calculate the patient's heart rate average based on the latest 10 tuples
heart_average_stream = heart_rate_stream.map(Avg(10))

# Print data stream
heart_average_stream.sink(print)

# Submit on Bluemix
submit(ContextTypes.ANALYTICS_SERVICE, topo, cfg)
