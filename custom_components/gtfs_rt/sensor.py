import datetime
import logging
import requests
import time
from enum import Enum

import voluptuous as vol

from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import (CONF_NAME, ATTR_LONGITUDE, ATTR_LATITUDE)
import homeassistant.util.dt as dt_util
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
import homeassistant.helpers.config_validation as cv

_LOGGER = logging.getLogger(__name__)

ATTR_STOP_ID = "Stop ID"
ATTR_ROUTE = "Route"
ATTR_DUE_IN = "Due in"
ATTR_DUE_AT = "Due at"
ATTR_OCCUPANCY = "Occupancy"
ATTR_NEXT_UP = "Next service"
ATTR_NEXT_UP_DUE_IN = "Next service due in"
ATTR_NEXT_UP_DUE_IN = "Next service due in"
ATTR_NEXT_ROUTE = "Next Route"
ATTR_NEXT_OCCUPANCY = "Next service occupancy"

CONF_API_KEY = 'api_key'
CONF_APIKEY = 'apikey'
CONF_X_API_KEY = 'x_api_key'
CONF_STOP_ID = 'stopid'
CONF_ROUTE = 'route'
CONF_DEPARTURES = 'departures'
CONF_DESTINATION = 'destination'
CONF_CONNECTIONS = 'connections'
CONF_TRIP_UPDATE_URL = 'trip_update_url'
CONF_VEHICLE_POSITION_URL = 'vehicle_position_url'
CONF_MIN_WALKING_TIME = 'min_walking_time'
CONF_ROUTE_TYPE = 'route_type'

DEFAULT_NAME = 'Next Service'

MIN_TIME_BETWEEN_UPDATES = datetime.timedelta(seconds=60)
TIME_STR_FORMAT = "%H:%M"


PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_TRIP_UPDATE_URL): cv.string,
    vol.Optional(CONF_API_KEY): cv.string,
    vol.Optional(CONF_X_API_KEY): cv.string,
    vol.Optional(CONF_APIKEY): cv.string,
    vol.Optional(CONF_VEHICLE_POSITION_URL): cv.string,
    vol.Optional(CONF_DEPARTURES): [{
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Required(CONF_STOP_ID): cv.string,
        vol.Required(CONF_ROUTE): cv.string,
        vol.Optional(CONF_ROUTE_TYPE, default='bus'): cv.string,
        vol.Optional(CONF_MIN_WALKING_TIME, default='0'): cv.string
    }],
    vol.Optional(CONF_CONNECTIONS): [{
        vol.Required(CONF_NAME): cv.string,
        vol.Required(CONF_STOP_ID): cv.string,
        vol.Required(CONF_DESTINATION): cv.string,
        vol.Optional(CONF_ROUTE_TYPE, default='bus'): cv.string,
        vol.Optional(CONF_MIN_WALKING_TIME, default='0'): cv.string
    }]
})

class OccupancyStatus(Enum):
    EMPTY = 0
    MANY_SEATS_AVAILABLE = 1
    FEW_SEATS_AVAILABLE = 2
    STANDING_ROOM_ONLY = 3
    CRUSHED_STANDING_ROOM_ONLY = 4
    FULL = 5
    NOT_ACCEPTING_PASSENGERS = 6
    NO_DATA_AVAILABLE = 7
    NOT_BOARDABLE = 8

class RouteType(Enum):
    # maps readable names to GTFS's route_type
    # this may be helpful for route_type detection in the future
    TRAM = 0
    SUBWAY = 1
    TRAIN = 2
    BUS = 3
    FERRY = 4
    CABLETRAM = 5
    GONDOLA = 6
    FUNICULAR = 7
    TROLLEYBUS = 11
    MONORAIL = 12

def due_in_minutes(timestamp):
    """Get the remaining minutes from now until a given datetime object."""
    diff = timestamp - dt_util.now().replace(tzinfo=None)
    return int(diff.total_seconds() / 60)


def setup_platform(hass, config, add_devices, discovery_info=None):
    """Get the public transport sensor."""

    data = PublicTransportData(config.get(CONF_TRIP_UPDATE_URL), config.get(CONF_VEHICLE_POSITION_URL), config.get(CONF_API_KEY), config.get(CONF_X_API_KEY), config.get(CONF_APIKEY))
    sensors = []
    if config.get(CONF_DEPARTURES) is not None:
        for departure in config.get(CONF_DEPARTURES):
            sensors.append(PublicTransportSensor(
                data,
                departure.get(CONF_STOP_ID),
                departure.get(CONF_ROUTE),
                departure.get(CONF_NAME),
                departure.get(CONF_ROUTE_TYPE),
                departure.get(CONF_MIN_WALKING_TIME)
            ))
    if config.get(CONF_CONNECTIONS) is not None:
        for connection in config.get(CONF_CONNECTIONS):
            sensors.append(PublicTransportSensorDestination(
                data,
                connection.get(CONF_STOP_ID),
                connection.get(CONF_DESTINATION),
                connection.get(CONF_NAME),
                connection.get(CONF_ROUTE_TYPE),
                connection.get(CONF_MIN_WALKING_TIME)
            ))

    add_devices(sensors)


class PublicTransportSensor(Entity):
    """Implementation of a public transport sensor."""

    def __init__(self, data, stop, route, name, routetype, min_walking_time):
        """Initialize the sensor."""
        self.data = data
        self._name = name
        self._stop = stop
        self._route = route
        self._routetype = RouteType[routetype.upper()]
        self.min_walking_time = float(min_walking_time)
        self.update()

    @property
    def name(self):
        return self._name

    def _get_next_buses(self):
        depart = []
        for trip in self.data.info.get(self._route, {}):
            if self._stop in trip:
                # skip connections from the past and that can't be reached in the very near future
                if due_in_minutes(trip[self._stop].arrival_time) < self.min_walking_time:
                    continue
                depart.append(trip[self._stop])

        # Sort by arrival time
        depart.sort(key = lambda t: t.arrival_time)

        return depart

    @property
    def state(self):
        """Return the state of the sensor."""
        next_buses = self._get_next_buses()
        return due_in_minutes(next_buses[0].arrival_time) if len(next_buses) > 0 else '-'

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        next_buses = self._get_next_buses()
        attrs = {
            ATTR_DUE_IN: self.state,
            ATTR_STOP_ID: self._stop,
        }
        if len(next_buses) > 0:
            attrs[ATTR_ROUTE] = next_buses[0].route_id
            attrs[ATTR_DUE_AT] = next_buses[0].arrival_time.strftime(TIME_STR_FORMAT) if len(next_buses) > 0 else '-'
            attrs[ATTR_OCCUPANCY] = next_buses[0].occupancy
            if next_buses[0].position:
                attrs[ATTR_LATITUDE] = next_buses[0].position.latitude
                attrs[ATTR_LONGITUDE] = next_buses[0].position.longitude
        if len(next_buses) > 1:
            attrs[ATTR_NEXT_ROUTE] = next_buses[1].route_id
            attrs[ATTR_NEXT_UP] = next_buses[1].arrival_time.strftime(TIME_STR_FORMAT) if len(next_buses) > 1 else '-'
            attrs[ATTR_NEXT_UP_DUE_IN] = due_in_minutes(next_buses[1].arrival_time) if len(next_buses) > 1 else '-'
            attrs[ATTR_NEXT_OCCUPANCY] = next_buses[1].occupancy
        return attrs

    @property
    def unit_of_measurement(self):
        """Return the unit this state is expressed in."""
        return "min"

    @property
    def icon(self):
        """Icon to use in the frontend, if any."""
        if self._routetype == RouteType.TRAM:
            return 'mdi:tram'
        if self._routetype == RouteType.SUBWAY:
            return 'mdi:subway'
        if self._routetype == RouteType.TRAIN:
            return 'mdi:train'
        if self._routetype == RouteType.BUS:
            return 'mdi:bus'
        if self._routetype == RouteType.FERRY:
            return 'mdi:ferry'
        if self._routetype == RouteType.CABLETRAM:
            return 'mdi:tram'  # no mdi icon found for cable tram
        if self._routetype == RouteType.GONDOLA:
            return 'mdi:gondola'
        if self._routetype == RouteType.FUNICULAR:
            return 'mdi:tram'  # no mdi icon found for funicular
        if self._routetype == RouteType.TROLLEYBUS:
            return 'mdi:bus'  # no mdi icon found for trolleybus
        if self._routetype == RouteType.MONORAIL:
            return 'mdi:train' # no mdi icon found for mono-rail
        return 'mdi:bus'  # 'else'

    def update(self):
        """Get the latest data from opendata.ch and update the states."""
        self.data.update()


class PublicTransportSensorDestination(PublicTransportSensor):
    def __init__(self, data, stop, destination, name, routetype, min_walking_time):
        """Initialize the sensor."""
        self.data = data
        self._name = name
        self._stop = stop
        self._destination = destination
        self._routetype = RouteType[routetype.upper()]
        self.min_walking_time = float(min_walking_time)
        self.update()

    def _get_next_buses(self):
        # now implement the new functionality:
        # find all connections from stop_id towards a dest_id
        # this is of course a more demanding task since we need to go through
        # all the data
        # this probably does not really work with circle lines
        # departure_times[route_id][-1][stop_id]
        connections = []
        for line_id, trips in self.data.info.items():
            for trip in trips:
                if self._stop in trip and self._destination in trip:
                    # skip connections from the past and that can't be reached in the very near future
                    if due_in_minutes(trip[self._stop].arrival_time) < self.min_walking_time:
                        continue
                    stops = list(trip.keys())
                    # select correct travel direction
                    if stops.index(self._destination) > stops.index(self._stop):
                        connections.append(trip[self._stop])

        # Sort by arrival time
        connections.sort(key = lambda t: t.arrival_time)

        return connections


class PublicTransportData(object):
    """The Class for handling the data retrieval."""

    def __init__(self, trip_update_url, vehicle_position_url=None, api_key=None, x_api_key=None, apikey=None):
        """Initialize the info object."""
        self._trip_update_url = trip_update_url
        self._vehicle_position_url = vehicle_position_url
        if api_key is not None:
            self._headers = {'Authorization': api_key}
        elif apikey is not None:
            self._headers = {'apikey': apikey}    
        elif x_api_key is not None:
            self._headers = {'x-api-key': x_api_key}
        else:
            self._headers = None
        self.info = {}

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    def update(self):
        positions, vehicles_trips, occupancy = self._get_vehicle_positions() if self._vehicle_position_url else [{}, {}, {}]
        self._update_route_statuses(positions, vehicles_trips, occupancy)

    def _update_route_statuses(self, vehicle_positions, vehicles_trips, vehicle_occupancy):
        """Get the latest data."""
        from google.transit import gtfs_realtime_pb2

        class StopDetails:
            def __init__(self, route_id, arrival_time, position, occupancy):
                self.route_id = str(route_id)
                self.arrival_time = arrival_time
                self.position = position
                self.occupancy = occupancy

        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self._trip_update_url, headers=self._headers)
        if response.status_code != 200:
            _LOGGER.error("updating route status got {}:{}".format(response.status_code,response.content))
        feed.ParseFromString(response.content)
        departure_times = {}

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                route_id = entity.trip_update.trip.route_id

                # Get link between vehicle_id from trip_id from vehicles positions if needed
                vehicle_id = entity.trip_update.vehicle.id
                if not vehicle_id:
                    vehicle_id = vehicles_trips.get(entity.trip_update.trip.trip_id)

                if route_id not in departure_times:
                    departure_times[route_id] = []

                departure_times[route_id].append({})

                for stop in entity.trip_update.stop_time_update:
                    stop_id = stop.stop_id
                    # Use stop departure time; fall back on stop arrival time if not available
                    details = StopDetails(
                        route_id,  # a bit redundant here, but helpful later
                        datetime.datetime.fromtimestamp(stop.arrival.time),
                        vehicle_positions.get(vehicle_id),
                        vehicle_occupancy.get(vehicle_id)
                    )
                    departure_times[route_id][-1][stop_id] = details

        self.info = departure_times

    def _get_vehicle_positions(self):
        from google.transit import gtfs_realtime_pb2
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self._vehicle_position_url, headers=self._headers)
        if response.status_code != 200:
            _LOGGER.error("updating vehicle positions got {}:{}.".format(response.status_code, response.content))
        feed.ParseFromString(response.content)
        positions = {}
        vehicles_trips = {}
        occupancy = {}

        for entity in feed.entity:
            vehicle = entity.vehicle
            if not vehicle.trip.route_id:
                # Vehicle is not in service
                continue
            positions[vehicle.vehicle.id] = vehicle.position
            vehicles_trips[vehicle.trip.trip_id] = vehicle.vehicle.id
            occupancy[vehicle.vehicle.id] = OccupancyStatus(vehicle.occupancy_status).name
        return positions, vehicles_trips, occupancy
