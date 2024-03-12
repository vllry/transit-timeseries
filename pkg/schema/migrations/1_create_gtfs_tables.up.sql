CREATE TABLE IF NOT EXISTS gtfs_agencies (
    -- TODO: add feed identifier
    agency_id TEXT NOT NULL,
    valid_period tsrange NOT NULL,
    agency_name TEXT NOT NULL,
    agency_url TEXT NOT NULL,
    agency_timezone TEXT NOT NULL,
    agency_lang TEXT,
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT,
    PRIMARY KEY (agency_id, valid_period)
);

CREATE TABLE IF NOT EXISTS gtfs_stops (
    feed_agency_id TEXT NOT NULL,
    valid_period tsrange NOT NULL,
    stop_id TEXT,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_desc TEXT,
    position GEOGRAPHY(Point, 4326) NOT NULL,
    -- stop_lat DOUBLE PRECISION NOT NULL,
    -- stop_lon DOUBLE PRECISION NOT NULL,
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    stop_timezone TEXT,
    wheelchair_boarding INTEGER,
    level_id TEXT,
    platform_code TEXT,
    PRIMARY KEY (feed_agency_id, valid_period, stop_id)
);

CREATE TABLE IF NOT EXISTS gtfs_routes (
    feed_agency_id TEXT NOT NULL,
    valid_period tsrange NOT NULL,
    route_id TEXT NOT NULL,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER NOT NULL,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT,
    route_sort_order INTEGER,
    PRIMARY KEY (feed_agency_id, valid_period, route_id)
);

CREATE TABLE IF NOT EXISTS gtfs_trips (
    feed_agency_id TEXT NOT NULL,
    valid_period tsrange NOT NULL,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER,
    PRIMARY KEY (feed_agency_id, valid_period, trip_id)
);

CREATE TABLE IF NOT EXISTS gtfs_stop_times (
    feed_agency_id TEXT NOT NULL,
    valid_period tsrange NOT NULL,
    trip_id TEXT NOT NULL,
    arrival_time TIME,
    departure_time TIME,
    stop_id TEXT,
    stop_sequence INTEGER NOT NULL,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled DOUBLE PRECISION,
    timepoint INTEGER,
    PRIMARY KEY (feed_agency_id, valid_period, trip_id, stop_sequence)
);
