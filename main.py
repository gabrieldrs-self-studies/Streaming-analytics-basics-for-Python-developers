from streamsx.topology.topology import Topology
from streamsx.topology import schema
from streamsx.topology.context import *
from streams.observations import observation_stream  # Cannot use "as X"

import hashlib

service_name = "Streaming Analytics-no"  # Service name
credentials = {}  # Put redentials here

def anonymize(tuple_):
    tuple_['patientId'] = hashlib.sha256(tuple_['patientId'].encode('utf - 8')).digest()
    tuple_['device']['locationId'] = hashlib.sha256(tuple_['device']['locationId'].encode('utf - 8')).digest()

    return tuple_


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

# Print data stream
heart_rate_stream.sink(print)

# Submit on Bluemix
submit(ContextTypes.ANALYTICS_SERVICE, topo, cfg)
