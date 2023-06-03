package cdp

import (
	"encoding/json"
	"log"
	"net/http"

	"xegments.com/cdp/dataaccess"

	"github.com/gorilla/mux"
)

type Event struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

func startGateway() {
	// Create a new router using Gorilla Mux
	router := mux.NewRouter()

	// Define the route for receiving event data
	router.HandleFunc("/events", handleEvent).Methods("POST")

	// Start the server
	log.Println("Data Collection Gateway is running on port 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	// Parse the JSON payload from the request body
	var event Event
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Store the event data in the PostgreSQL database using the DataAccessLayer
	err = dataaccess.DataAccessLayer.CreateData("events", event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with a success message
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Event received and stored successfully"))
}
