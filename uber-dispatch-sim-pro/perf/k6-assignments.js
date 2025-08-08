// k6 script: load the /assignments endpoint
import http from 'k6/http';
import { sleep } from 'k6';
export const options = { vus: 50, duration: '1m' };
export default function () {
  http.get('http://localhost:8080/assignments');
  sleep(0.5);
}
