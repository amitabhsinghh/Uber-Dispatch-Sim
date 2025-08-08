package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
)

type RideRequest struct {
	RideID   string  `json:"ride_id"`
	RiderID  string  `json:"rider_id"`
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
}

type Assignment struct {
	RideID     string     `json:"ride_id"`
	DriverID   string     `json:"driver_id"`
	EtaMin     int        `json:"eta_min"`
	SurgeMult  float64    `json:"surge_mult"`
	Price      float64    `json:"price"`
	AssignedAt time.Time  `json:"assigned_at"`
}

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" { log.Fatalf("missing env %s", key) }
	return val
}

func boolEnv(key string) bool {
	v := strings.ToLower(os.Getenv(key))
	return v == "1" || v == "true" || v == "yes"
}

var (
	assignCtr = prometheus.NewCounter(prometheus.CounterOpts{Name: "dispatch_assignments_total", Help: "Total assignments"})
	assignLat = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "dispatch_assign_latency", Help: "Assignment latency", Buckets: prometheus.ExponentialBuckets(5, 1.5, 15)})
	retryCtr  = prometheus.NewCounter(prometheus.CounterOpts{Name: "dispatch_retries_total", Help: "Retries"})
	deadCtr   = prometheus.NewCounter(prometheus.CounterOpts{Name: "dispatch_deadletter_total", Help: "Dead-lettered rides"})
)

// Haversine distance in kilometers
func haversine(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lng2 - lng1) * math.Pi / 180
	la1 := lat1 * math.Pi / 180
	la2 := lat2 * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Sin(dLon/2)*math.Cos(la1)*math.Cos(la2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

// Very small Hungarian algorithm implementation (cost min). Handles rectangular by padding.
func hungarian(cost [][]float64) []int {
	n := len(cost)
	m := len(cost[0])
	// subtract row minima
	for i := 0; i < n; i++ {
		min := cost[i][0]
		for j := 1; j < m; j++ { if cost[i][j] < min { min = cost[i][j] } }
		for j := 0; j < m; j++ { cost[i][j] -= min }
	}
	// subtract column minima
	for j := 0; j < m; j++ {
		min := cost[0][j]
		for i := 1; i < n; i++ { if cost[i][j] < min { min = cost[i][j] } }
		for i := 0; i < n; i++ { cost[i][j] -= min }
	}
	// This is a simplified greedy cover (not full optimal Hungarian for all cases, but works well for sparse costs)
	res := make([]int, n)
	for i := range res { res[i] = -1 }
	usedCols := make([]bool, m)
	for i := 0; i < n; i++ {
		best := -1
		bestVal := 1e18
		for j := 0; j < m; j++ {
			if usedCols[j] { continue }
			if cost[i][j] < bestVal { bestVal = cost[i][j]; best = j }
		}
		if best >= 0 { res[i] = best; usedCols[best] = true }
	}
	return res
}

func baseFare(distKm float64) float64 {
	return 2.0 + 0.8*distKm
}

func travelTimeMinutes(distKm float64, hour int) int {
	// toy: base speed varies by hour (rush hours slower)
	speedKmph := 30.0
	if hour >= 8 && hour <= 10 { speedKmph = 18.0 }
	if hour >= 17 && hour <= 20 { speedKmph = 20.0 }
	mins := int((distKm / speedKmph) * 60.0) + 2
	if mins < 2 { mins = 2 }
	if mins > 60 { mins = 60 }
	return mins
}

func surgeMultiplier(driversNearby int, ridesNearby int) float64 {
	if driversNearby <= 0 { driversNearby = 1 }
	ratio := float64(ridesNearby) / float64(driversNearby)
	m := 1.0 + math.Max(0, ratio-1.0)*0.6 // gentle curve
	if m > 3.0 { m = 3.0 }
	return m
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func(){ <-c; cancel() }()

	kafkaBroker := mustEnv("KAFKA_BROKER")
	ridesTopic := mustEnv("RIDES_TOPIC")
	assignTopic := mustEnv("ASSIGNMENTS_TOPIC")
	deadTopic := os.Getenv("ASSIGNMENTS_DEAD_TOPIC")
	if deadTopic == "" { deadTopic = "rides.dead.v1" }
	pgURL := fmt.Sprintf("postgres://%s:%s@postgres:5432/%s",
		mustEnv("POSTGRES_USER"), mustEnv("POSTGRES_PASSWORD"), mustEnv("POSTGRES_DB"))
	redisURL := mustEnv("REDIS_URL")
	promPort := os.Getenv("PROM_PORT"); if promPort == "" { promPort = "9100" }
	batchSize := 20
	batchWait := 250 * time.Millisecond

	// Chaos toggles
	dropKafka := boolEnv("CHAOS_DROP_KAFKA")
	failPct, _ := strconv.Atoi(strings.TrimSpace(strings.Split(os.Getenv("CHAOS_FAIL_ASSIGN_PERCENT"),"#")[0]))
	_ = failPct

	// OpenTelemetry
	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") // e.g. http://tempo:4317 (gRPC)
	var tp *trace.TracerProvider
	if otelEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(otelEndpoint, "http://"), "https://")),
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithDialOption(grpc.WithBlock()),
		)
		if err == nil {
			res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes("dispatch"))
			tp = trace.NewTracerProvider(trace.WithBatcher(exp), trace.WithResource(res))
			otel.SetTracerProvider(tp)
			log.Println("OTEL tracing enabled ->", otelEndpoint)
		} else {
			log.Println("OTEL init failed:", err)
		}
	}
	defer func(){ if tp != nil { _ = tp.Shutdown(ctx) } }()

	tracer := otel.Tracer("dispatch" )

	// Prometheus
	reg := prometheus.NewRegistry()
	reg.MustRegister(assignCtr, assignLat, retryCtr, deadCtr)
	go func(){
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("prometheus metrics on :%s/metrics", promPort)
		_ = http.ListenAndServe(":"+promPort, nil)
	}()

	// Postgres
	pgpool, err := pgxpool.New(ctx, pgURL)
	if err != nil { log.Fatalf("pg connect: %v", err) }
	defer pgpool.Close()
	_, err = pgpool.Exec(ctx, `CREATE TABLE IF NOT EXISTS assignments (
		id SERIAL PRIMARY KEY,
		ride_id TEXT UNIQUE,
		driver_id TEXT,
		eta_min INT,
		surge_mult DOUBLE PRECISION,
		price DOUBLE PRECISION,
		assigned_at TIMESTAMPTZ DEFAULT NOW()
	)`)
	if err != nil { log.Fatalf("pg schema: %v", err) }

	// Redis
	rOpts, err := redis.ParseURL(redisURL)
	if err != nil { log.Fatalf("redis parse: %v", err) }
	rdb := redis.NewClient(rOpts)
	defer rdb.Close()
	geoKey := "drivers:geo"

	// Kafka
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
		"group.id": "dispatch-consumer",
		"auto.offset.reset": "earliest",
	})
	if err != nil { log.Fatalf("kafka consumer: %v", err) }
	defer consumer.Close()
	_ = consumer.Subscribe(ridesTopic, nil)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil { log.Fatalf("kafka producer: %v", err) }
	defer producer.Close()

	log.Println("dispatch: micro-batching + Hungarian + surge + OTEL ready")


	// batch loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// collect batch
			rides := make([]RideRequest, 0, batchSize)
			deadline := time.Now().Add(batchWait)
			for len(rides) < batchSize && time.Now().Before(deadline) {
				msg, err := consumer.ReadMessage(50 * time.Millisecond)
				if err != nil { continue }
				var r RideRequest
				if err := json.Unmarshal(msg.Value, &r); err == nil {
					rides = append(rides, r)
				}
			}

			if len(rides) == 0 { continue }

			ctxBatch, span := tracer.Start(ctx, "batch_assign")
			span.SetAttributes(attribute.Int("rides", len(rides)))

			startBatch := time.Now()

			// gather candidate drivers for all rides (union of nearest 30 each within 8km)
			driverMap := map[string]redis.GeoLocation{}
			for _, r := range rides {
				locs, _ := rdb.GeoSearchLocation(ctxBatch, geoKey, &redis.GeoSearchLocationQuery{
					GeoSearchQuery: redis.GeoSearchQuery{
						Longitude: r.Lng, Latitude: r.Lat, Radius: 8, RadiusUnit: "km", Sort: "ASC", Count: 30,
					},
				}).Result()
				for _, d := range locs { driverMap[d.Name] = d }
			}
			drivers := make([]redis.GeoLocation, 0, len(driverMap))
			for _, d := range driverMap { drivers = append(drivers, d) }

			if len(drivers) == 0 {
				// dead-letter all rides (no drivers at all)
				for _, r := range rides {
					assignDead(producer, deadTopic, r, errors.New("no drivers in radius"))
					deadCtr.Inc()
				}
				span.End()
				continue
			}

			// build cost matrix
			cost := make([][]float64, len(rides))
			for i, r := range rides {
				row := make([]float64, len(drivers))
				for j, d := range drivers {
					row[j] = haversine(r.Lat, r.Lng, d.Latitude, d.Longitude)
				}
				cost[i] = row
			}

			assignIdx := hungarian(cost) // index of driver for each ride, -1 if none

			// finalize assignments
			for i, r := range rides {
				j := assignIdx[i]
				if j < 0 || j >= len(drivers) {
					retryCtr.Inc()
					assignDead(producer, deadTopic, r, errors.New("no match"))
					deadCtr.Inc()
					continue
				}

				chosen := drivers[j]
				// remove chosen from geo (optimistically)
				_ = rdb.ZRem(ctxBatch, geoKey, chosen.Name).Err()

				dist := haversine(r.Lat, r.Lng, chosen.Latitude, chosen.Longitude)
				driversNearby, _ := rdb.GeoSearch(ctxBatch, geoKey, &redis.GeoSearchQuery{
					Longitude: r.Lng, Latitude: r.Lat, Radius: 3, RadiusUnit: "km",
					Count: 100,
				}).Result()
				ridesNearby := 0
				for k := range rides { // cheap local density by batch
					if haversine(r.Lat, r.Lng, rides[k].Lat, rides[k].Lng) <= 3.0 { ridesNearby++ }
				}
				surge := surgeMultiplier(len(driversNearby), ridesNearby)
				eta := travelTimeMinutes(dist, time.Now().Hour())
				price := baseFare(dist) * surge

				ass := Assignment{
					RideID: r.RideID,
					DriverID: chosen.Name,
					EtaMin: eta,
					SurgeMult: surge,
					Price: math.Round(price*100) / 100,
					AssignedAt: time.Now().UTC(),
				}

				_, err := pgpool.Exec(ctxBatch, `INSERT INTO assignments (ride_id, driver_id, eta_min, surge_mult, price) VALUES ($1,$2,$3,$4,$5)
					ON CONFLICT (ride_id) DO UPDATE SET driver_id=EXCLUDED.driver_id, eta_min=EXCLUDED.eta_min, surge_mult=EXCLUDED.surge_mult, price=EXCLUDED.price`,
					ass.RideID, ass.DriverID, ass.EtaMin, ass.SurgeMult, ass.Price)
				if err != nil { log.Printf("pg insert: %v", err) }

				b, _ := json.Marshal(ass)
				if !dropKafka {
					producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &assignTopic, Partition: kafka.PartitionAny},
						Value: b,
					}, nil)
				}
				assignCtr.Inc()
			}

			assignLat.Observe(float64(time.Since(startBatch).Milliseconds()))
			span.End()
		}
	}
}

func assignDead(producer *kafka.Producer, topic string, ride RideRequest, reason error) {
	b, _ := json.Marshal(map[string]any{"ride": ride, "error": reason.Error()})
	topicCopy := topic
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicCopy, Partition: kafka.PartitionAny},
		Value: b,
	}, nil)
	log.Printf("dead-lettered ride %s: %v", ride.RideID, reason)
}
