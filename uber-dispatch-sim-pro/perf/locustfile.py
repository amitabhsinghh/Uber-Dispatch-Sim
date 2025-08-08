from locust import HttpUser, task, between

class GatewayUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(3)
    def assignments(self):
        self.client.get("/assignments")

    @task(1)
    def health(self):
        self.client.get("/health")
