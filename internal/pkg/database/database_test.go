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

func TestThatTrafficFlowObservedCanBeCreatedAndRetrieved(t *testing.T) {
	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	src := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T11:10:00.000Z", 1, 127)
	src.RefRoadSegment = types.NewSingleObjectRelationship("refRoadSegment")
	src.Location = geojson.CreateGeoJSONPropertyFromWGS84(17.310863, 62.389109)

	_, err := db.CreateTrafficFlowObserved(&src)
	if err != nil {
		t.Errorf("Something went wrong when creating new TrafficFlowObserved: %s", err)
	}

	_, err = db.GetTrafficFlowsObserved(3)
	if err != nil {
		t.Errorf("Something went wrong when retrieving TrafficFlowsObserved: %s", err)
	}
}

func TestThatGetTrafficFlowObservedReturnsCorrectAmountOfEntries(t *testing.T) {
	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	fiwareTfos := []fiware.TrafficFlowObserved{}
	src1 := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T11:10:00.000Z", 1, 35)
	src2 := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T11:30:00.000Z", 3, 34)
	src3 := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T12:30:00.000Z", 2, 420)
	src4 := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T13:30:00.000Z", 6, 50)
	src5 := *fiware.NewTrafficFlowObserved("urn:ngsi-ld:TrafficFlowObserved", "2016-12-07T14:30:00.000Z", 0, 3)

	fiwareTfos = append(fiwareTfos, src1, src2, src3, src4, src5)

	for _, tfo := range fiwareTfos {
		_, err := db.CreateTrafficFlowObserved(&tfo)
		if err != nil {
			t.Errorf("Something went wrong when creating new TrafficFlowObserved: %s", err)
		}
	}

	tfos, _ := db.GetTrafficFlowsObserved(3)
	if len(tfos) < 3 || len(tfos) > 3 {
		t.Errorf("GetTrafficFlowsObserved retrieved an unexpectd amount of entries, got: %d, expected 3", len(tfos))
	}

	tfosBytes, _ := json.MarshalIndent(tfos, " ", "  ")
	log.Infoln(string(tfosBytes))
}

func TestCreateTrafficFlowObservedFailsOnEmptyDateObserved(t *testing.T) {
	db, _ := db.NewDatabaseConnection(db.NewSQLiteConnector(), nil)

	tfo := fiware.TrafficFlowObserved{}
	json.Unmarshal([]byte(tfoJsonNoDate), &tfo)

	_, err := db.CreateTrafficFlowObserved(&tfo)
	if err == nil {
		t.Errorf("Nothing went wrong when creating new TrafficFlowObserved: %s", err.Error())
	}
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
