import time


def observation_stream():
    while True:
        dictObj = {
            "patientId":"patient-1", "device": {
                "id":"VitalsGenerator",
                "locationId":"bed1"
            },
            "readingSource": {
                "id":123,
                "deviceId":"VitalsGenerator",
                "sourceType":"generated"
            },
            "reading": {
                "ts": 605,
                "uom":"bpm",
                "value":82.56785326532197,
                "readingType": {
                    "code":"8867-4",
                    "system":"streamsx.health/1.0"
                }
            }
        }
        time.sleep(1) # 1 second
        yield dictObj