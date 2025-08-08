// Sustained traffic scenario
import http from 'k6/http';
import { sleep } from 'k6';
export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 40, timeUnit: '1s',
      duration: '3m',
      preAllocatedVUs: 40, maxVUs: 200,
    },
  },
};
export default function () {
  http.get('http://localhost:8080/assignments');
  sleep(0.2);
}
