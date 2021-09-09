package database_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	db "github.com/diwise/api-transportation/internal/pkg/database"
	"github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	"github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/geojson"
	"github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/types"
	log "github.com/sirupsen/logrus"

	"github.com/matryer/is"
)

func TestMain(m *testing.M) {
	log.SetFormatter(&log.JSONFormatter{})
	os.Exit(m.Run())
}

func TestSeedSingleRoad(t *testing.T) {
	seedData := "21277:153930;21277:153930;62.389109;17.310863;62.389084;17.310852\n"

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	if db.GetRoadCount() != 1 {
		t.Error("Unexpected number of roads in datastore after test.", 1, "!=", db.GetRoadCount())
	}
}

func TestSeedDatabase(t *testing.T) {
	seedData := "21277:153930;21277:153930;62.389109;17.310863;62.389084;17.310852;62.389073;17.310854;62.389059;17.310878;62.389057;17.310897;62.389052;17.310940\n"

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	_, err := db.GetRoadByID("21277:153930")
	if err != nil {
		t.Error("Unable to find expected road from id:", err.Error())
	}
}

func TestGetRoadSegmentNearPoint(t *testing.T) {
	seedData := "21277:153930;21277:153930;62.389109;17.310863;62.389084;17.310852;62.389073;17.310854;62.389059;17.310878;62.389057;17.310897;62.389052;17.310940\n"

	datastore, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	segments, _ := datastore.GetSegmentsNearPoint(62.389077, 17.310243, 75)
	if len(segments) == 0 {
		t.Error("Unable to find segments near a point. None returned.")
	}
}

func TestGetRoadSegmentsWithinRect(t *testing.T) {
	seedData := "21277:153930;21277:153930;62.389109;17.310863;62.389084;17.310852;62.389073;17.310854;62.389059;17.310878;62.389057;17.310897;62.389052;17.310940\n"

	datastore, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	segments, _ := datastore.GetSegmentsWithinRect(62.389077, 17.310243, 62.4, 17.4)
	if len(segments) == 0 {
		t.Error("Unable to find segments near a point. None returned.")
	}
}

func TestBoundingBoxCreation(t *testing.T) {
	r1 := db.NewRectangle(db.NewPoint(1, 1), db.NewPoint(2, 2))
	r2 := db.NewRectangle(db.NewPoint(1, 3), db.NewPoint(2, 4))
	r3 := db.NewRectangle(db.NewPoint(3, 1), db.NewPoint(4, 2))
	r4 := db.NewRectangle(db.NewPoint(3, 3), db.NewPoint(4, 4))

	box := db.NewBoundingBoxFromRectangles(r1, r2)
	if db.NewPoint(1.5, 2.5).IsBoundedBy(&box) == false {
		t.Error("Failed!")
	}

	box = db.NewBoundingBoxFromRectangles(r1, r3)
	if db.NewPoint(2, 1.5).IsBoundedBy(&box) == false {
		t.Error("Failed!")
	}

	box = db.NewBoundingBoxFromRectangles(r4, r1)
	if db.NewPoint(2.5, 2.5).IsBoundedBy(&box) == false {
		t.Error("Failed!")
	}
}

func TestUpdateRoadSegmentSurface(t *testing.T) {
	segmentID := "21277:153930"
	seedData := fmt.Sprintf("%s;%s;62.389109;17.310863;62.389084;17.310852\n", segmentID, segmentID)
	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	db.RoadSegmentSurfaceUpdated(segmentID, "snow", 75.0, time.Now())

	seg, _ := db.GetRoadSegmentByID(segmentID)
	surfaceType, probability := seg.SurfaceType()

	if surfaceType != "snow" || probability != 75.0 || seg.DateModified() == nil {
		t.Errorf("Failed to update road segment surface type. %s (%f) did not match expectations.", surfaceType, probability)
	}
}

var theDawnOfTime time.Time
var theEndOfTime time.Time

func TestThatTrafficFlowObservedCanBeCreatedAndRetrieved(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	src := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T11:10:00.000Z", 1, 127)
	src.RefRoadSegment = types.NewSingleObjectRelationship("refRoadSegment")
	src.Location = geojson.CreateGeoJSONPropertyFromWGS84(17.310863, 62.389109)

	_, err := db.CreateTrafficFlowObserved(&src)
	is.NoErr(err)

	_, err = db.GetTrafficFlowsObserved(theDawnOfTime, theEndOfTime, 3)
	is.NoErr(err)
}

func TestThatGetTrafficFlowObservedReturnsCorrectAmountOfEntriesInTheCorrectOrder(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	fiwareTfos := []fiware.TrafficFlowObserved{}
	src1 := *fiware.NewTrafficFlowObserved("ignored0", "2016-12-07T11:10:00.000Z", 1, 35)
	src2 := *fiware.NewTrafficFlowObserved("ignored1", "2016-12-07T11:30:00.000Z", 3, 34)
	src3 := *fiware.NewTrafficFlowObserved("third", "2016-12-07T12:30:00.000Z", 2, 420)
	src4 := *fiware.NewTrafficFlowObserved("second", "2016-12-07T13:30:00.000Z", 6, 50)
	src5 := *fiware.NewTrafficFlowObserved("first", "2016-12-07T14:30:00.000Z", 0, 3)

	fiwareTfos = append(fiwareTfos, src1, src2, src3, src4, src5)

	for _, tfo := range fiwareTfos {
		_, err := db.CreateTrafficFlowObserved(&tfo)
		is.NoErr(err)
	}

	tfos, _ := db.GetTrafficFlowsObserved(theDawnOfTime, theEndOfTime, 3)
	is.Equal(len(tfos), 3) // unexpected number of observations returned

	suffixes := []string{"first", "second", "third"}

	for i, tfo := range tfos {
		is.True(strings.HasSuffix(tfo.TrafficFlowObservedID, suffixes[i])) // results returned in the wrong order
	}
}

func TestThatGetTrafficFlowObservedHandlesSelectFromTime(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	fiwareTfos := []fiware.TrafficFlowObserved{}
	src1 := *fiware.NewTrafficFlowObserved("ignored0", "2016-12-07T11:10:00.000Z", 1, 35)
	src2 := *fiware.NewTrafficFlowObserved("ignored1", "2016-12-07T11:30:00.000Z", 3, 34)
	src3 := *fiware.NewTrafficFlowObserved("third", "2016-12-07T12:30:00.000Z", 2, 420)
	src4 := *fiware.NewTrafficFlowObserved("second", "2016-12-07T13:30:00.000Z", 6, 50)
	src5 := *fiware.NewTrafficFlowObserved("first", "2016-12-07T14:30:00.000Z", 0, 3)

	fiwareTfos = append(fiwareTfos, src1, src2, src3, src4, src5)

	for _, tfo := range fiwareTfos {
		_, err := db.CreateTrafficFlowObserved(&tfo)
		is.NoErr(err)
	}

	fromTime, _ := time.Parse(time.RFC3339, "2016-12-07T13:00:00.000Z")

	tfos, _ := db.GetTrafficFlowsObserved(fromTime, theEndOfTime, 10)
	is.Equal(len(tfos), 2) // only expected the last two records
}

func TestThatGetTrafficFlowObservedHandlesSelectBeforeTime(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	fiwareTfos := []fiware.TrafficFlowObserved{}
	src1 := *fiware.NewTrafficFlowObserved("ignored0", "2016-12-07T11:10:00.000Z", 1, 35)
	src2 := *fiware.NewTrafficFlowObserved("ignored1", "2016-12-07T11:30:00.000Z", 3, 34)
	src3 := *fiware.NewTrafficFlowObserved("third", "2016-12-07T12:30:00.000Z", 2, 420)
	src4 := *fiware.NewTrafficFlowObserved("second", "2016-12-07T13:30:00.000Z", 6, 50)
	src5 := *fiware.NewTrafficFlowObserved("first", "2016-12-07T14:30:00.000Z", 0, 3)

	fiwareTfos = append(fiwareTfos, src1, src2, src3, src4, src5)

	for _, tfo := range fiwareTfos {
		_, err := db.CreateTrafficFlowObserved(&tfo)
		is.NoErr(err)
	}

	endTime, _ := time.Parse(time.RFC3339, "2016-12-07T11:15:00.000Z")

	tfos, _ := db.GetTrafficFlowsObserved(theDawnOfTime, endTime, 10)
	is.Equal(len(tfos), 1) // only expected the first record
}

func TestThatGetTrafficFlowObservedHandlesSelectBetweenTimes(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	fiwareTfos := []fiware.TrafficFlowObserved{}
	src1 := *fiware.NewTrafficFlowObserved("ignored0", "2016-12-07T11:10:00.000Z", 1, 35)
	src2 := *fiware.NewTrafficFlowObserved("ignored1", "2016-12-07T11:30:00.000Z", 3, 34)
	src3 := *fiware.NewTrafficFlowObserved("third", "2016-12-07T12:30:00.000Z", 2, 420)
	src4 := *fiware.NewTrafficFlowObserved("second", "2016-12-07T13:30:00.000Z", 6, 50)
	src5 := *fiware.NewTrafficFlowObserved("first", "2016-12-07T14:30:00.000Z", 0, 3)

	fiwareTfos = append(fiwareTfos, src1, src2, src3, src4, src5)

	for _, tfo := range fiwareTfos {
		_, err := db.CreateTrafficFlowObserved(&tfo)
		is.NoErr(err)
	}

	startTime, _ := time.Parse(time.RFC3339, "2016-12-07T11:15:00.000Z")
	endTime, _ := time.Parse(time.RFC3339, "2016-12-07T14:00:00.000Z")

	tfos, _ := db.GetTrafficFlowsObserved(startTime, endTime, 10)
	is.Equal(len(tfos), 3) // only expected the three middle records
}

func TestCreateTrafficFlowObservedFailsOnEmptyDateObserved(t *testing.T) {
	is := is.New(t)

	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	tfo := fiware.TrafficFlowObserved{}
	json.Unmarshal([]byte(tfoJsonNoDate), &tfo)

	_, err := db.CreateTrafficFlowObserved(&tfo)
	is.True(err != nil) // unexpected success when creating new TrafficFlowObserved
}

const tfoJsonNoDate string = `{
    "@context": [
      "https://schema.lab.fiware.org/ld/context",
      "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    ],
    "id": "urn:ngsi-ld:TrafficFlowObserved:sn-tcr-01:test",
    "type": "TrafficFlowObserved",
		"location": {
			"type": "GeoProperty",
			"value": {
				"coordinates": [
					17.0,
					62.2
				],
			"type": "Point"
			}
		},
		"dateObserved": {
			"type": "Property",
			"value": {
				"type": "DateTime",
				"value": "2016-12-07T11:10:00Z/2016-12-07T11:15:00Z"
			}
		},
		"laneID": {
			"type": "Property",
			"value": 1
		},
		"averageVehicleSpeed": {
			"type": "Property",
			"value": 17.3
		},
		"intensity": {
			"type": "Property",
			"value": 8
		},
		"refRoadSegment": {
			"type": "Relationship",
			"object": ""
		}
}`

func TestConnectToSQLite(t *testing.T) {
	segmentID := "21277:153930"
	seedData := fmt.Sprintf("%s;%s;62.389109;17.310863;62.389084;17.310852\n", segmentID, segmentID)
	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), strings.NewReader(seedData))

	err := db.UpdateRoadSegmentSurface(segmentID, "snow", 75.0, time.Now())

	if err != nil {
		t.Errorf("Failed to update road segment surface type in database. %s", err.Error())
	}

	err = db.UpdateRoadSegmentSurface(segmentID, "tarmac", 85.0, time.Now())

	if err != nil {
		t.Errorf("Failed to update road segment surface type a second time in database. %s", err.Error())
	}
}
