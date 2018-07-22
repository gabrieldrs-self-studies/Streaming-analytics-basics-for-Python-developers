from streamsx.topology.topology import Topology
from streamsx.topology import schema
from streamsx.topology.context import *
from streams.observations import observation_stream  # Cannot use "as X"

import hashlib
from collections import deque
import threading

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

def data_collecter(view):
    for d in iter(view.get, None):
        plot_queue.append(float(d))


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

# Creating view
heart_average_view = heart_average_stream.view()

# Print data stream
heart_average_stream.sink(print)

# Submit on Bluemix
submit(ContextTypes.ANALYTICS_SERVICE, topo, cfg)

view = heart_average_view.start_data_fetch()

plot_queue = deque([], 2000)

data_pull_thread = threading.Thread(target=data_collecter, args=(view, ))
data_pull_thread.start()

import time
from IPython import display
# For matlab to work properly on mac, you have to install Python as framework, with pyenv run:
#  env PYTHON_CONFIGURE_OPTS="--enable-framework CC=clang" pyenv install 3.5.5
from matplotlib import pylab as pl

pl.rcParams['figure.figsize'] = (14.0, 8.0)

while (True):
    pl.clf()
    ax = pl.gca()
    ax.set_autoscale_on(False)
    ax.plot(plot_queue)
    ax.axis([0, 2000, 50, 120])
    display.display(pl.gcf())
    print(len(plot_queue))
    display.clear_output(wait=True)
    time.sleep(1.0)