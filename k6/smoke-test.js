import http from 'k6/http';
import { check } from 'k6';
import { describe, expect } from 'https://jslib.k6.io/k6chaijs/4.3.4.3/index.js';

const baseUrl = `${__ENV.TEST_SVC_BASEURL}`;

export const options = {
  //discardResponseBodies: true,

  thresholds: {
    // 90% of requests must finish within 400ms.
    http_req_duration: ['p(90) < 200'],
  },

//  scenarios: {
//    contacts: {
//      executor: 'ramping-arrival-rate',
//
//      // Start iterations per `timeUnit`
//      startRate: 100,
//      timeUnit: '1s',
//
//      // Pre-allocate necessary VUs.
//      preAllocatedVUs: 150,
//
//      stages: [
//        { target: 200, duration: '10s' },
//        { target: 200, duration: '60s' },
//        { target: 210, duration: '30s' },
//        { target: 220, duration: '30s' },
//        { target: 190, duration: '60s' },
//        { target: 100, duration: '30s' },
//        { target: 30, duration: '30s' },
//      ],
//    },
//  },

  scenarios: {
    contacts: {
      executor: 'constant-arrival-rate',
      duration: '120s',
      rate: 200,
      timeUnit: '1s',

      // Pre-allocate necessary VUs.
      preAllocatedVUs: 350,
    },
  },

//  scenarios: {
//    contacts: {
//      executor: 'ramping-arrival-rate',
//
//      // Start iterations per `timeUnit`
//      startRate: 40,
//      timeUnit: '1s',
//
//      // Pre-allocate necessary VUs.
//      preAllocatedVUs: 150,
//
//      stages: [
//        { target: 50, duration: '30s' },
//        { target: 60, duration: '60s' },
//        { target: 80, duration: '30s' },
//        { target: 90, duration: '30s' },
//        { target: 99, duration: '30s' },
//        { target: 100, duration: '60s' },
//        { target: 150, duration: '60s' },
//        { target: 200, duration: '180s' },
//        { target: 50, duration: '60s' },
//        { target: 30, duration: '60s' },
//      ],
//    },
//  },

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
