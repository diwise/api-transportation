package handler

import (
	"compress/flate"
	"net/http"
	"os"

	"github.com/diwise/api-transportation/internal/pkg/database"
	fiwarecontext "github.com/diwise/api-transportation/internal/pkg/fiware/context"
	"github.com/diwise/messaging-golang/pkg/messaging"
	ngsi "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"

	"github.com/rs/cors"

	log "github.com/sirupsen/logrus"
)

//RequestRouter wraps the concrete router implementation
type RequestRouter struct {
	impl *chi.Mux
}

func (router *RequestRouter) addNGSIHandlers(contextRegistry ngsi.ContextRegistry) {
	router.Get("/ngsi-ld/v1/entities", ngsi.NewQueryEntitiesHandler(contextRegistry))
	router.Post("/ngsi-ld/v1/entities", ngsi.NewCreateEntityHandler(contextRegistry))
	router.Patch("/ngsi-ld/v1/entities/{entity}/attrs/", ngsi.NewUpdateEntityAttributesHandler(contextRegistry))
}

func (router *RequestRouter) addProbeHandlers() {
	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func (router *RequestRouter) Patch(pattern string, handlerFn http.HandlerFunc) {
	router.impl.Patch(pattern, handlerFn)
}

func (router *RequestRouter) Post(pattern string, handlerFn http.HandlerFunc) {
	router.impl.Post(pattern, handlerFn)
}

func (router *RequestRouter) Get(pattern string, handlerFn http.HandlerFunc) {
	router.impl.Get(pattern, handlerFn)
}

func newRequestRouter() *RequestRouter {
	router := &RequestRouter{impl: chi.NewRouter()}

	router.impl.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	// Enable gzip compression for ngsi-ld responses
	compressor := middleware.NewCompressor(flate.DefaultCompression, "application/json", "application/ld+json")
	router.impl.Use(compressor.Handler)
	router.impl.Use(middleware.Logger)

	return router
}

func createRequestRouter(contextRegistry ngsi.ContextRegistry) *RequestRouter {
	router := newRequestRouter()

	router.addProbeHandlers()
	router.addNGSIHandlers(contextRegistry)

	return router
}

//MessagingContext is an interface that allows mocking of messaging.Context parameters
type MessagingContext interface {
	PublishOnTopic(message messaging.TopicMessage) error
	NoteToSelf(message messaging.CommandMessage) error
}

//CreateRouterAndStartServing creates a request router, registers all handlers and starts serving requests.
func CreateRouterAndStartServing(messenger MessagingContext, db database.Datastore) {

	contextRegistry := ngsi.NewContextRegistry()
	ctxSource := fiwarecontext.CreateSource(db, messenger)
	contextRegistry.Register(ctxSource)

	router := createRequestRouter(contextRegistry)

	port := os.Getenv("TRANSPORTATION_API_PORT")
	if port == "" {
		port = "8484"
	}

	log.Printf("Starting api-transportation on port %s.\n", port)

	log.Fatal(http.ListenAndServe(":"+port, router.impl))
}
