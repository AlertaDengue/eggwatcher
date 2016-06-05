import unittest
from influxdb import InfluxDBClient
from datetime import date, datetime
import csv
import copy

template = {
    "measurement": "egg_counts",
    "tags": {
        "Project": "SMS-RJ",
        "city": "RMRJ"
    },
    "time": "2009-11-10T23:00:00Z",
    "fields": {
        "eggs": 156,
        "otid": '1',
        "lat": 23.0,
        "lon": 46.0,
        "install_date": '2016-06-05T12:02:17.464167'
    }
}
json_body = [template]


class TestWrites(unittest.TestCase):
    def setUp(self):
        self.client = InfluxDBClient('localhost', 8086, 'root', 'root', 'Ovitrampas')
        # self.client.delete_series('Ovitrampas', 'egg_counts')
        self.db = self.client.create_database('Ovitrampas')
        self.data = csv.DictReader(open('test_data.csv'), delimiter=';')

    def tearDown(self):
        pass
        del self.data

    def test_single_write(self):
        self.client.write_points(json_body)
        result = self.client.query('select eggs from egg_counts;')
        self.assertEqual(len([json_body]), len(result.raw['series']))

    def test_multiple_writes(self):
        points = []
        for d in self.data:
            datum = {"measurement": "egg_counts",
                     "tags": {
                         "Project": "SMS-RJ",
                         "city": "RMRJ"
                     },
                     'fields': {}}
            datum['fields']['otid'] = d['NUM']
            datum['fields']['lat'] = 0.0 if not d['Y_LAT'] else float(d['Y_LAT'])
            datum['fields']['lon'] = 0.0 if not d['Y_LAT'] else float(d['X_LONG'])
            datum['fields']['install_date'] = date.today().isoformat() if not d[
                'Data de instalação'] else datetime.strptime(d['Data de instalação'], '%d/%m/%Y').isoformat()
            datum['fields']['eggs'] = 0 if not d['OVOS'] else int(d['OVOS'])
            datum['time'] = datetime.now().isoformat()
            # self.client.write_points([datum])
            points.append(datum)
        self.client.write_points(points)
