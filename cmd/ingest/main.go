package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vllry/transit-timeseries/pkg/proto/gtfsrt"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

var vehicleDescriptorsCache = make(map[VehicleDescriptor]int)
var vehicleDescriptorsLock = sync.RWMutex{}
var tripDescriptorsCache = make(map[TripDescriptor]int)
var tripDescriptorsLock = sync.RWMutex{}

// agencyTagSet is the set of agency tags that have been seen so far,
// to avoid excess lookup/insert attempts.
var agencyTagSet = make(map[string]struct{})

func main() {
	realTimeDirectory := flag.String("dir-rt", "", "Directory to ingest")
	staticDirectory := flag.String("dir-static", "", "Directory to ingest")
	eff := flag.String("eff", "", "Effective time")
	postgresString := flag.String("postgres", "", "Postgres connection string")

	flag.Parse()

	if *postgresString == "" {
		panic("Postgres connection string is required")
	}

	poolConfig, err := pgxpool.ParseConfig(*postgresString)
	if err != nil {
		panic(err)
	}
	poolConfig.MaxConns = 50

	db, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert static data
	if *staticDirectory != "" {
		// Convert string time eff to UTC time.Time
		effectiveTime, err := time.Parse("2006-01-02 15:04:05", *eff)
		if err != nil {
			panic(err)
		}
		err = insertGtfsStatic(db, *staticDirectory, effectiveTime)
		if err != nil {
			panic(err)
		}
		return
	}

	// Otherwise...

	if *realTimeDirectory == "" {
		panic("Directory is required")
	}

	paths, err := WalkDir(*realTimeDirectory)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	// Limit the number of goroutines running concurrently to maxGoroutines.
	maxGoroutines := 70
	// Create a buffered channel to act as a semaphore.
	sem := make(chan struct{}, maxGoroutines)

	for i := range paths {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore
		go func(path string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			err := parseFileAndInsert(db, path)
			if err != nil {
				fmt.Println(path, err)
			}

			fmt.Println(path)
		}(paths[len(paths)-i-1])
	}

	wg.Wait()
}

// WalkDir walks the directory tree rooted at root, returning a list of all file paths
func WalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasSuffix(info.Name(), ".7zip") {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return files, nil
}

func parseFileAndInsert(db *pgxpool.Pool, file string) error {
	// Read the file
	f, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}

	err = insertVehiclePositions(db, f)
	if err != nil {
		fmt.Println(err)
		// Write the file to a temp directory for debugging
		err = os.WriteFile(fmt.Sprintf("tmp/%s", filepath.Base(file)), f, 0644)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func insertVehiclePositions(conn *pgxpool.Pool, b []byte) error {
	fm := new(gtfsrt.FeedMessage)
	err := proto.Unmarshal(b, fm)
	if err != nil {
		return err
	}

	for _, entity := range fm.GetEntity() {
		vehicle := entity.GetVehicle()
		if vehicle == nil {
			continue
		}

		if vehicle.GetTrip() == nil {
			fmt.Println("Vehicle has no trip")
			continue
		}
		agency_tag := strings.Split(*vehicle.GetTrip().RouteId, ":")[0]

		// Skip non-AC agencies for now
		if agency_tag != "AC" && agency_tag != "ac" {
			continue
		}

		// Insert new agencies by tag
		if _, found := agencyTagSet[agency_tag]; !found {
			_, err := conn.Exec(
				context.Background(),
				"INSERT INTO agencies (id) VALUES ($1) ON CONFLICT DO NOTHING",
				agency_tag,
			)
			if err != nil {
				return err
			}
			agencyTagSet[agency_tag] = struct{}{}
		}

		// Conditionally insert the vehicle descriptor, if it is not yet known to us.
		// This may result in duplicate attempts to INSERT, which is fine.

		vd := VehicleDescriptor{
			AgencyID:     agency_tag,
			VehicleID:    vehicle.GetVehicle().GetId(),
			LicensePlate: vehicle.GetVehicle().GetLicensePlate(),
			VehicleLabel: vehicle.GetVehicle().GetLabel(),
		}

		vehicleDescriptorsLock.RLock()
		vehicleDescriptorID, found := vehicleDescriptorsCache[vd]
		vehicleDescriptorsLock.RUnlock()

		if !found {
			err := func() error {
				vehicleDescriptorsLock.Lock() // We don't care about the race condition where the program attempts to double-insert some vehicles
				defer vehicleDescriptorsLock.Unlock()

				// Insert the vehicle descriptor
				_, err := conn.Exec(
					context.Background(),
					"INSERT INTO gtfsrt_vehicle_descriptors (agency_id, vehicle_id, license_plate, vehicle_label) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
					agency_tag, vehicle.GetVehicle().GetId(), vehicle.GetVehicle().GetLicensePlate(), vehicle.GetVehicle().GetLabel(),
				)
				if err != nil {
					return err
				}

				// Fetch the vehicle descriptor ID
				err = conn.QueryRow(
					context.Background(),
					"SELECT id FROM gtfsrt_vehicle_descriptors WHERE agency_id = $1 AND vehicle_id = $2 AND license_plate = $3 AND vehicle_label = $4",
					agency_tag, vehicle.GetVehicle().GetId(), vehicle.GetVehicle().GetLicensePlate(), vehicle.GetVehicle().GetLabel(),
				).Scan(&vehicleDescriptorID)
				if err != nil {
					return err
				}

				// Cache the vehicle descriptor ID
				vehicleDescriptorsCache[vd] = vehicleDescriptorID

				return nil
			}()

			if err != nil {
				return err
			}
		}

		// Conditionally insert the trip descriptor, if it is not yet known to us.

		td := TripDescriptor{
			AgencyID:             agency_tag,
			TripID:               vehicle.GetTrip().GetTripId(),
			RouteID:              vehicle.GetTrip().GetRouteId(),
			DirectionID:          vehicle.GetTrip().GetDirectionId(),
			StartTime:            vehicle.GetTrip().GetStartTime(),
			StartDate:            vehicle.GetTrip().GetStartDate(),
			ScheduleRelationship: vehicle.GetTrip().GetScheduleRelationship().String(),
		}

		tripDescriptorsLock.RLock()
		tripDescriptorID, found := tripDescriptorsCache[td]
		tripDescriptorsLock.RUnlock()

		if !found {
			err := func() error {
				tripDescriptorsLock.Lock()
				defer tripDescriptorsLock.Unlock()

				// Insert the trip descriptor
				_, err = conn.Exec(
					context.Background(),
					"INSERT INTO gtfsrt_trip_descriptors (agency_id, trip_id, route_id, direction_id, start_time, start_date, schedule_relationship) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
					agency_tag, vehicle.GetTrip().GetTripId(), vehicle.GetTrip().GetRouteId(), vehicle.GetTrip().GetDirectionId(), vehicle.GetTrip().GetStartTime(), vehicle.GetTrip().GetStartDate(), vehicle.GetTrip().GetScheduleRelationship(),
				)
				if err != nil {
					return err
				}

				// Fetch the trip descriptor ID
				err = conn.QueryRow(
					context.Background(),
					"SELECT id FROM gtfsrt_trip_descriptors WHERE agency_id = $1 AND trip_id = $2 AND route_id = $3 AND direction_id = $4 AND start_time = $5 AND start_date = $6 AND schedule_relationship = $7",
					agency_tag, vehicle.GetTrip().GetTripId(), vehicle.GetTrip().GetRouteId(), vehicle.GetTrip().GetDirectionId(), vehicle.GetTrip().GetStartTime(), vehicle.GetTrip().GetStartDate(), vehicle.GetTrip().GetScheduleRelationship(),
				).Scan(&tripDescriptorID)
				if err != nil {
					return err
				}

				// Cache the trip descriptor ID
				tripDescriptorsCache[td] = tripDescriptorID

				return nil
			}()

			if err != nil {
				return err
			}
		}

		// Insert the position using the vehicle descriptor ID and trip descriptor ID
		_, err = conn.Exec(
			context.Background(),
			"INSERT INTO gtfsrt_vehicle_positions (position, agency_id, vehicle_descriptor_id, trip_descriptor_id, bearing, odometer, speed, current_stop_sequence, stop_id, current_status, timestamp, congestion_level, occupancy_status) VALUES (ST_SetSRID(ST_MakePoint($1, $2), 4326), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ON CONFLICT (agency_id, vehicle_descriptor_id, timestamp) DO UPDATE SET speed = EXCLUDED.speed, odometer = EXCLUDED.odometer, bearing = EXCLUDED.bearing",
			vehicle.GetPosition().GetLongitude(), vehicle.GetPosition().GetLatitude(), agency_tag, vehicleDescriptorID, tripDescriptorID, vehicle.GetPosition().Bearing, vehicle.GetPosition().Odometer, vehicle.GetPosition().Speed, vehicle.GetCurrentStopSequence(), vehicle.GetStopId(), vehicle.GetCurrentStatus(), vehicle.GetTimestamp(), vehicle.GetCongestionLevel(), vehicle.GetOccupancyStatus(),
		)
		if err != nil {
			return err
		}
	}

	return nil
}
