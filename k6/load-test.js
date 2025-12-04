import http from 'k6/http';
import { check } from 'k6';
import { describe, expect } from 'https://jslib.k6.io/k6chaijs/4.3.4.3/index.js';

const baseUrl = `${__ENV.TEST_SVC_BASEURL}`;

export const options = {
  //discardResponseBodies: true,

  scenarios: {
    contacts: {
      executor: 'ramping-arrival-rate',

      // Start iterations per `timeUnit`
      startRate: 10,
      timeUnit: '1s',

      // Pre-allocate necessary VUs.
      preAllocatedVUs: 50,

      stages: [
        { target: 30, duration: '20s' },
        { target: 50, duration: '20s' },
        { target: 60, duration: '20s' },
        { target: 80, duration: '20s' },
        { target: 90, duration: '20s' },
        { target: 99, duration: '20s' },
        { target: 100, duration: '20s' },
        { target: 50, duration: '20s' },
        { target: 30, duration: '20s' },
        { target: 100, duration: '20s' },
        { target: 200, duration: '20s' },
        { target: 300, duration: '20s' },
        { target: 400, duration: '20s' },
        { target: 500, duration: '20s' },
        { target: 600, duration: '30s' },
        { target: 100, duration: '20s' },
      ],
    },
  },
};

export default function testSuite() {
  describe('smoke test', () => {
    const options = {
        headers: {
            //Authorization: `Basic ${encodedCredentials}`,
        },
    };

    const url = `${baseUrl}/livez?verbose`;
    const response = http.get(url, options);

    check(response, {
        'is status 200': (r) => r.status == 200,
        'is status 429 (rate limited)': (r) => r.status == 429,
        'is status 503 (rate limited)': (r) => r.status == 503,
    });

    expect(response.status, 'response status').to.equal(200);
    expect(response).to.have.validJsonBody();
    //console.log(response.json());

//    const responses = http.batch([
//        ['GET', url],
//        ['GET', url],
//        ['GET', url],
//        ['GET', url],
//        ['GET', url],
//    ]);
//    check(responses[0], {
//        'status was 200': (res) => res.status === 200,
//    });
  });
}
