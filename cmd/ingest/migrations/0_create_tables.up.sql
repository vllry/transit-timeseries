-- Create tables to store gtfsrt vehicle positions, trip updates, and service alerts.
-- This includes supporting tables to reduce duplication of data and to acomadate 1:many relationships.

CREATE TYPE gtfsrt_schedule_relationship AS ENUM ('SCHEDULED', 'ADDED', 'UNSCHEDULED', 'CANCELED', 'REPLACED');
CREATE TYPE gtfsrt_congestion_level AS ENUM ('UNKNOWN_CONGESTION_LEVEL', 'RUNNING_SMOOTHLY', 'STOP_AND_GO', 'CONGESTION', 'SEVERE_CONGESTION');
CREATE TYPE gtfsrt_occupancy_status AS ENUM ('EMPTY', 'MANY_SEATS_AVAILABLE', 'FEW_SEATS_AVAILABLE', 'STANDING_ROOM_ONLY', 'CRUSHED_STANDING_ROOM_ONLY', 'FULL', 'NOT_ACCEPTING_PASSENGERS');
CREATE TYPE gtfsrt_vehicle_stop_status AS ENUM ('INCOMING_AT', 'STOPPED_AT', 'IN_TRANSIT_TO');

CREATE TABLE agencies (
    id TEXT PRIMARY KEY, -- Synthetic key, manually assigned in configuration.
    agency_name TEXT
);

CREATE TABLE gtfsrt_vehicle_descriptors (
    -- https://gtfs.org/realtime/reference/#message-vehicledescriptor
    agency_id TEXT NOT NULL,
    id SERIAL PRIMARY KEY, -- Synthetic key, because vehicles don't have a single unique identifier.

    vehicle_id TEXT,
    license_plate TEXT,
    vehicle_label TEXT, -- By making this unique, we allow duplicate entires for the same vehicle if the label changes.
    -- This is a sign that this should be two tables.

    FOREIGN KEY (agency_id) REFERENCES agencies(id),
    UNIQUE NULLS NOT DISTINCT (agency_id, vehicle_id, license_plate, vehicle_label)
);

CREATE TABLE gtfsrt_trip_descriptors (
    -- https://gtfs.org/realtime/reference/#message-tripdescriptor
    agency_id TEXT NOT NULL,
    id SERIAL PRIMARY KEY,

    trip_id TEXT,
    route_id TEXT,
    start_date TEXT,
    start_time TEXT,
    direction_id INT,
    schedule_relationship gtfsrt_schedule_relationship,

    FOREIGN KEY (agency_id) REFERENCES agencies(id),
    UNIQUE NULLS NOT DISTINCT (agency_id, trip_id, route_id, start_date, start_time, direction_id, schedule_relationship)
);

CREATE TABLE gtfsrt_vehicle_positions (
    -- https://developers.google.com/transit/gtfs-realtime/reference#message-vehicleposition

    agency_id TEXT NOT NULL REFERENCES agencies(id),

    trip_descriptor_id INT NOT NULL REFERENCES gtfsrt_trip_descriptors(id),
    vehicle_descriptor_id INT NOT NULL REFERENCES gtfsrt_vehicle_descriptors(id),

    -- Position fields
    position GEOGRAPHY(Point, 4326) NOT NULL,
    bearing FLOAT,
    odometer BIGINT,
    speed FLOAT,

    -- Status fields
    current_stop_sequence INT,
    stop_id TEXT,
    current_status gtfsrt_vehicle_stop_status,
    congestion_level gtfsrt_congestion_level,
    occupancy_status gtfsrt_occupancy_status,

    -- Timestamps
    timestamp INT NOT NULL,
    -- last_seen INT NOT NULL,
    
    PRIMARY KEY (agency_id, vehicle_descriptor_id, timestamp)
);

CREATE INDEX idx_gtfsrt_vehicle_positions_position
ON gtfsrt_vehicle_positions
USING GIST (position);

/*
CREATE TABLE gtfsrt_stop_time_updates (
    -- https://developers.google.com/transit/gtfs-realtime/reference#message-stop-time-update

    stop_sequence int,
    stop_id TEXT,

    -- Arrival StopTimeEvent fields
    arrival_delay INT,
    arrival_time INT,
    arrival_uncertainty INT,

    -- Departure StopTimeEvent fields
    departure_delay INT,
    departure_time INT,
    departure_uncertainty INT,

    schedule_relationship gtfsrt_schedule_relationship,
);

CREATE TABLE gtfsrt_trip_updates (
    agency_id INT NOT NULL,

    -- TripDescriptor fields
    trip_id TEXT,
    route_id TEXT,
    start_date TEXT,
    start_time TEXT,
    direction_id INT,

    -- VehicleDescriptor fields
    vehicle_id TEXT,
    license_plate TEXT,
    vehicle_label TEXT,

    -- stop_time_updates in a trip_update are a 1:many relationship.
    -- Join on gtfsrt_trip_updates
    
    timestamp INT,
    delay INT,

    FOREIGN KEY (agency_id) REFERENCES agencies(agency_id),
);
*/

/*

CREATE TYPE gtfsrt_service_alert_cause AS ENUM ('UNKNOWN_CAUSE', 'OTHER_CAUSE', 'TECHNICAL_PROBLEM', 'STRIKE', 'DEMONSTRATION', 'ACCIDENT', 'HOLIDAY', 'WEATHER', 'MAINTENANCE', 'CONSTRUCTION', 'POLICE_ACTIVITY', 'MEDICAL_EMERGENCY');
CREATE TYPE gtfsrt_service_alert_effect AS ENUM ('NO_SERVICE', 'REDUCED_SERVICE', 'SIGNIFICANT_DELAYS', 'DETOUR', 'ADDITIONAL_SERVICE', 'MODIFIED_SERVICE', 'OTHER_EFFECT', 'UNKNOWN_EFFECT', 'STOP_MOVED');
CREATE TYPE gtfsrt_schedule_relationship AS ENUM ('SCHEDULED', 'ADDED', 'UNSCHEDULED', 'CANCELED', 'REPLACED');

CREATE TABLE gtfsrt_service_alerts (
    -- https://developers.google.com/transit/gtfs-realtime/reference#message-alert
    -- Contains compund types []active_period, []informed_entity.

    -- Use a synthetic key, because alerts don't have a unique identifier.
    alert_id SERIAL PRIMARY KEY,

    agency_id TEXT NOT NULL,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL,

    -- active_period
    active_period_start INT,
    active_period_end INT,

    -- Alerts may target a specific route, trip, or stop.
    route_id TEXT,
    route_type INT,
    direction_id INT,
    stop_id TEXT,

    -- These are trip-specific fields, if the alert only targets a trip.
    -- https://developers.google.com/transit/gtfs-realtime/reference#message-tripdescriptor
    trip_id TEXT,
    trip_start_time TEXT,
    trip_start_date TEXT,
    trip_schedule_relationship gtfsrt_service_alert_schedule_relationship,

    cause gtfsrt_service_alert_cause,
    effect gtfsrt_service_alert_effect,

    url TEXT,
    header TEXT NOT NULL,
    description TEXT NOT NULL,

    UNIQUE (agency_id, active_period_time_start, active_period_time_end, route_id, route_type, direction_id, stop_id, trip_id, trip_start_time, trip_start_date, trip_schedule_relationship, cause, effect, url, header, description)
);

-- ServiceAlerts can have multiple active periods.
CREATE TABLE gtfsrt_service_alert_times (
    alert_id INT NOT NULL,
    start_time INT NOT NULL,
    end_time INT NOT NULL,
    PRIMARY KEY (alert_id, start_time, end_time)
);

CREATE TABLE gtfsrt_informed_entity (
    agency_id TEXT, -- Note: this is a gtfsrt field, not our agency_id.
    route_id TEXT,
    route_type INT,
    trip_id TEXT,
    trip_id TEXT,
    trip_start_time TEXT,
    trip_start_date TEXT,
    trip_schedule_relationship gtfsrt_service_alert_schedule_relationship,
    stop_id TEXT,

    UNIQUE NULLS NOT DISTINCT (alert_id, agency_id, route_id, route_type, trip_id, stop_id)
);
*/