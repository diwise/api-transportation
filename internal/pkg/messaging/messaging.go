package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/diwise/api-transportation/internal/pkg/database"
	"github.com/diwise/api-transportation/internal/pkg/messaging/commands"
	"github.com/diwise/api-transportation/internal/pkg/messaging/events"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/streadway/amqp"
)

//MessagingContext is an interface that allows mocking of messaging.Context parameters
type MessagingContext interface {
	PublishOnTopic(message messaging.TopicMessage) error
	NoteToSelf(message messaging.CommandMessage) error
}

//CreateRoadSegmentSurfaceUpdatedReceiver is a closure that take a datastore and handles incoming events
func CreateRoadSegmentSurfaceUpdatedReceiver(db database.Datastore) messaging.TopicMessageHandler {
	return func(msg amqp.Delivery) {
		log.Infof("message received from topic: %s", string(msg.Body))

		evt := &events.RoadSegmentSurfaceUpdated{}
		err := json.Unmarshal(msg.Body, evt)

		if err != nil {
			log.Errorf("failed to unmarshal message: %s", err.Error())
			return
		}

		ts, err := time.Parse(time.RFC3339, evt.Timestamp)
		if err != nil {
			log.Errorf("failed to parse event timestamp %s", evt.Timestamp)
			return
		}

		err = db.RoadSegmentSurfaceUpdated(evt.ID, evt.SurfaceType, evt.Probability, ts)

		if err != nil {
			log.Errorf("failed to update road segment surface: %s", err.Error())
			return
		}
	}
}

//CreateUpdateRoadSegmentSurfaceCommandHandler returns a handler for commands
func CreateUpdateRoadSegmentSurfaceCommandHandler(db database.Datastore, msg MessagingContext) messaging.CommandHandler {
	return func(wrapper messaging.CommandMessageWrapper) error {
		cmd := &commands.UpdateRoadSegmentSurface{}
		err := json.Unmarshal(wrapper.Body(), cmd)
		if err != nil {
			return fmt.Errorf("failed to unmarshal command: %s", err.Error())
		}

		ts, err := time.Parse(time.RFC3339, cmd.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to parse command timestamp %s", cmd.Timestamp)
		}

		err = db.UpdateRoadSegmentSurface(cmd.ID, cmd.SurfaceType, cmd.Probability, ts)
		if err != nil {
			return fmt.Errorf("failed to update road segment surface: %s", err.Error())
		}

		//Post an event stating that a roadsegment's surface has been updated
		event := &events.RoadSegmentSurfaceUpdated{
			ID:          cmd.ID,
			SurfaceType: cmd.SurfaceType,
			Probability: cmd.Probability,
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		}
		msg.PublishOnTopic(event)

		return nil
	}
}
