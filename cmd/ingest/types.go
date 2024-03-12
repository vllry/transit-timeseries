package main

type VehicleDescriptor struct {
	AgencyID     string
	VehicleID    string
	LicensePlate string
	VehicleLabel string
}

type TripDescriptor struct {
	AgencyID             string
	TripID               string
	RouteID              string
	DirectionID          uint32
	StartTime            string
	StartDate            string
	ScheduleRelationship string
}
