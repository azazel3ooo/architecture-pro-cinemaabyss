package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL      *url.URL
	moviesServiceURL *url.URL
	eventsServiceURL *url.URL
	gradualMigration bool
	migrationPercent int
)

func main() {
	initConfig()

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", proxyHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting proxy service on port %s", port)
	log.Printf("Gradual migration: %v, migration percent: %d%%", gradualMigration, migrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initConfig() {
	var err error

	monolithAddr := os.Getenv("MONOLITH_URL")
	if monolithAddr == "" {
		monolithAddr = "http://localhost:8080"
	}
	monolithURL, err = url.Parse(monolithAddr)
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesAddr := os.Getenv("MOVIES_SERVICE_URL")
	if moviesAddr == "" {
		moviesAddr = "http://localhost:8081"
	}
	moviesServiceURL, err = url.Parse(moviesAddr)
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	eventsAddr := os.Getenv("EVENTS_SERVICE_URL")
	if eventsAddr == "" {
		eventsAddr = "http://localhost:8082"
	}
	eventsServiceURL, err = url.Parse(eventsAddr)
	if err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	gradualMigration = os.Getenv("GRADUAL_MIGRATION") == "true"

	percentStr := os.Getenv("MOVIES_MIGRATION_PERCENT")
	if percentStr != "" {
		migrationPercent, err = strconv.Atoi(percentStr)
		if err != nil {
			log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: %v", err)
		}
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	var target *url.URL

	switch {
	case strings.HasPrefix(path, "/api/events"):
		target = eventsServiceURL
	case strings.HasPrefix(path, "/api/movies"):
		target = resolveMoviesTarget()
	default:
		target = monolithURL
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	log.Printf("Proxying %s %s -> %s", r.Method, path, target.String())
	proxy.ServeHTTP(w, r)
}

func resolveMoviesTarget() *url.URL {
	if !gradualMigration {
		return moviesServiceURL
	}

	if rand.Intn(100) < migrationPercent {
		log.Println("Routing to movies microservice")
		return moviesServiceURL
	}

	log.Println("Routing to monolith")
	return monolithURL
}
