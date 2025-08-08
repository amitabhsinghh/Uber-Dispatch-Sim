# Uber Dispatch Simulation

A distributed, Uber-style ride dispatch simulator that matches ride requests to the nearest available drivers at scale.  
Built with **Kafka**, **Redis**, **PostgreSQL**, **Go**, **Python**, and **Node.js**, with full observability via **Prometheus**, **Grafana**, and **Tempo**.

---

## Features

- **Driver Seeding** – Preload drivers with geo-coordinates into Redis for simulation.
- **Ride Dispatching** – Match riders to drivers using configurable strategies (`greedy`, `hungarian`).
- **Event Streaming** – Use Kafka topics for rides, assignments, and dead-letter queues.
- **Multi-Service Architecture** – Independent services for dispatch, gateway, and simulation.
- **Performance Testing** – k6 & Locust scripts for load and spike testing.
- **Observability** – Metrics via Prometheus, dashboards in Grafana, tracing with Tempo.
- **Dockerized Setup** – Fully containerized services for quick deployment.


