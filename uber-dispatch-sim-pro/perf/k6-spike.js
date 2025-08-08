// Spiky burst scenario
import http from 'k6/http';
export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 10, timeUnit: '1s',
      preAllocatedVUs: 50, maxVUs: 500,
      stages: [
        { duration: '30s', target: 50 },
        { duration: '30s', target: 200 },
        { duration: '30s', target: 500 },
        { duration: '1m',  target: 50 },
      ],
    },
  },
};
export default function () { http.get('http://localhost:8080/assignments'); }
