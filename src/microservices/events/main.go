package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var producer sarama.SyncProducer

func main() {
	initKafka()
	defer producer.Close()

	topics := []string{"movie-events", "user-events", "payment-events"}
	go startConsumers(topics)

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initKafka() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	var err error
	for i := 0; i < 30; i++ {
		producer, err = sarama.NewSyncProducer(brokerList, config)
		if err == nil {
			log.Println("Successfully connected to Kafka")
			return
		}
		log.Printf("Failed to connect to Kafka (attempt %d/30): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("Failed to connect to Kafka after 30 attempts: %v", err)
}

func startConsumers(topics []string) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	for _, topic := range topics {
		go consumeTopic(consumer, topic)
	}

	select {}
}

func consumeTopic(consumer sarama.Consumer, topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to start consumer for topic %s: %v", topic, err)
		return
	}
	defer partitionConsumer.Close()

	log.Printf("Started consuming topic: %s", topic)
	for msg := range partitionConsumer.Messages() {
		log.Printf("[%s] Received event: partition=%d, offset=%d, value=%s",
			topic, msg.Partition, msg.Offset, string(msg.Value))
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func publishEvent(topic string, payload []byte) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	}
	return producer.SendMessage(msg)
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event["timestamp"] = time.Now().Format(time.RFC3339)
	event["type"] = "movie"

	payload, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	partition, offset, err := publishEvent("movie-events", payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Published movie event: partition=%d, offset=%d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event["type"] = "user"

	payload, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	partition, offset, err := publishEvent("user-events", payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Published user event: partition=%d, offset=%d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event["type"] = "payment"

	payload, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	partition, offset, err := publishEvent("payment-events", payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish event: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Published payment event: partition=%d, offset=%d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}
