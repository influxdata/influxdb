// Funcs
import {
  isSystemBucket,
  getSortedBuckets,
  SYSTEM,
} from 'src/buckets/selectors/index'

// Types
import {AppState, Bucket} from 'src/types'

describe('Bucket Selector', () => {
  it('should return true when a default bucket is passed', () => {
    expect(isSystemBucket(SYSTEM)).toEqual(true)
  })
  it('should return false when no default bucket is passed', () => {
    expect(isSystemBucket(`_${SYSTEM}`)).toEqual(false)
    expect(isSystemBucket(`naming_${SYSTEM}`)).toEqual(false)
    expect(isSystemBucket('SYSTEM')).toEqual(false)
  })
  it('should sort the bucket names alphabetically', () => {
    const buckets: Bucket[] = [
      {
        id: '7902bd683453c00c',
        orgID: 'e483753c9bdb47bf',
        type: 'user',
        name: 'alpha',
        retentionRules: [],
        readableRetention: 'forever',
        createdAt: '2019-11-05T08:57:54.459819-08:00',
        updatedAt: '2019-11-05T08:58:09.593805-08:00',
        links: {
          labels: '/api/v2/buckets/7902bd683453c00c/labels',
          members: '/api/v2/buckets/7902bd683453c00c/members',
          org: '/api/v2/orgs/e483753c9bdb47bf',
          owners: '/api/v2/buckets/7902bd683453c00c/owners',
          self: '/api/v2/buckets/7902bd683453c00c',
          write: '/api/v2/write?org=e483753c9bdb47bf&bucket=7902bd683453c00c',
        },
        labels: [],
      },
      {
        id: '7f44462ac794c7c1',
        orgID: 'e483753c9bdb47bf',
        type: 'user',
        name: 'bucket1',
        retentionRules: [],
        readableRetention: 'forever',
        createdAt: '2019-10-15T11:10:27.970567-07:00',
        updatedAt: '2019-10-15T11:10:27.970567-07:00',
        links: {
          labels: '/api/v2/buckets/7f44462ac794c7c1/labels',
          members: '/api/v2/buckets/7f44462ac794c7c1/members',
          org: '/api/v2/orgs/e483753c9bdb47bf',
          owners: '/api/v2/buckets/7f44462ac794c7c1/owners',
          self: '/api/v2/buckets/7f44462ac794c7c1',
          write: '/api/v2/write?org=e483753c9bdb47bf&bucket=7f44462ac794c7c1',
        },
        labels: [],
      },
      {
        id: 'a8fee6b433c16f86',
        orgID: 'e483753c9bdb47bf',
        type: 'user',
        name: 'zebra',
        retentionRules: [],
        readableRetention: 'forever',
        createdAt: '2019-11-05T08:57:59.280485-08:00',
        updatedAt: '2019-11-05T08:57:59.280486-08:00',
        links: {
          labels: '/api/v2/buckets/a8fee6b433c16f86/labels',
          members: '/api/v2/buckets/a8fee6b433c16f86/members',
          org: '/api/v2/orgs/e483753c9bdb47bf',
          owners: '/api/v2/buckets/a8fee6b433c16f86/owners',
          self: '/api/v2/buckets/a8fee6b433c16f86',
          write: '/api/v2/write?org=e483753c9bdb47bf&bucket=a8fee6b433c16f86',
        },
        labels: [],
      },
      {
        id: 'adbb0107da2d7d38',
        orgID: 'e483753c9bdb47bf',
        type: 'user',
        name: 'buck2',
        retentionRules: [],
        readableRetention: 'forever',
        createdAt: '2019-10-18T14:05:24.838291-07:00',
        updatedAt: '2019-10-18T14:05:24.838292-07:00',
        links: {
          labels: '/api/v2/buckets/adbb0107da2d7d38/labels',
          members: '/api/v2/buckets/adbb0107da2d7d38/members',
          org: '/api/v2/orgs/e483753c9bdb47bf',
          owners: '/api/v2/buckets/adbb0107da2d7d38/owners',
          self: '/api/v2/buckets/adbb0107da2d7d38',
          write: '/api/v2/write?org=e483753c9bdb47bf&bucket=adbb0107da2d7d38',
        },
        labels: [],
      },
      {
        id: 'e2871ad8f92e752a',
        orgID: 'e483753c9bdb47bf',
        type: 'user',
        name: 'disco inferno',
        retentionRules: [],
        readableRetention: 'forever',
        createdAt: '2019-11-05T08:58:16.873502-08:00',
        updatedAt: '2019-11-05T08:58:16.873502-08:00',
        links: {
          labels: '/api/v2/buckets/e2871ad8f92e752a/labels',
          members: '/api/v2/buckets/e2871ad8f92e752a/members',
          org: '/api/v2/orgs/e483753c9bdb47bf',
          owners: '/api/v2/buckets/e2871ad8f92e752a/owners',
          self: '/api/v2/buckets/e2871ad8f92e752a',
          write: '/api/v2/write?org=e483753c9bdb47bf&bucket=e2871ad8f92e752a',
        },
        labels: [],
      },
      {
        id: '000000000000000a',
        type: 'system',
        description: 'System bucket for task logs',
        name: '_tasks',
        retentionRules: [
          {
            type: 'expire',
            everySeconds: 259200,
          },
        ],
        readableRetention: '3 days',
        createdAt: '0001-01-01T00:00:00Z',
        updatedAt: '0001-01-01T00:00:00Z',
        links: {
          labels: '/api/v2/buckets/000000000000000a/labels',
          members: '/api/v2/buckets/000000000000000a/members',
          org: '/api/v2/orgs/',
          owners: '/api/v2/buckets/000000000000000a/owners',
          self: '/api/v2/buckets/000000000000000a',
          write: '/api/v2/write?org=&bucket=000000000000000a',
        },
        labels: [],
      },
      {
        id: '000000000000000b',
        type: 'system',
        description: 'System bucket for monitoring logs',
        name: '_monitoring',
        retentionRules: [
          {
            type: 'expire',
            everySeconds: 604800,
          },
        ],
        readableRetention: '7 days',
        createdAt: '0001-01-01T00:00:00Z',
        updatedAt: '0001-01-01T00:00:00Z',
        links: {
          labels: '/api/v2/buckets/000000000000000b/labels',
          members: '/api/v2/buckets/000000000000000b/members',
          org: '/api/v2/orgs/',
          owners: '/api/v2/buckets/000000000000000b/owners',
          self: '/api/v2/buckets/000000000000000b',
          write: '/api/v2/write?org=&bucket=000000000000000b',
        },
        labels: [],
      },
    ]

    const results = getSortedBuckets({
      resources: {
        buckets: {
          status: 'Done',
          byID: buckets.reduce((prev, curr) => {
            prev[curr.id] = curr
            return prev
          }, {}),
          allIDs: buckets.map(bucket => bucket.id),
        },
      },
    } as AppState).map(bucket => bucket.name)
    const expectedResult = [
      'alpha',
      'buck2',
      'bucket1',
      'disco inferno',
      'zebra',
      '_monitoring',
      '_tasks',
    ]
    expect(results).toEqual(expectedResult)
  })
})
