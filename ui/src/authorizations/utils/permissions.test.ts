import {Authorization} from '@influxdata/influx'

import {filterIrrelevantAuths} from './permissions'

describe('filterIrrelevantAuths', () => {
  test('can find relevant tokens for a bucket', () => {
    const allAccessToken = {
      id: '03f4f29fd0011002',
      token: 'a',
      status: 'active',
      description: 'all access token',
      orgID: '03f4f29fd0011000',
      org: "Chris' Org",
      userID: '03f4f29fbd811000',
      user: 'chris',
      permissions: [
        {
          action: 'read',
          resource: {
            type: 'buckets',
          },
        },
        {
          action: 'write',
          resource: {
            type: 'buckets',
          },
        },
        {
          action: 'read',
          resource: {
            type: 'dashboards',
          },
        },
      ],
    } as Authorization

    const readAAndBBucketsToken = {
      id: '043f77872cad3000',
      token: 'b',
      status: 'active',
      description: 'scoped read token (to A and B)',
      orgID: '03f4f29fd0011000',
      org: "Chris' Org",
      userID: '03f4f29fbd811000',
      user: 'chris',
      permissions: [
        {
          action: 'read',
          resource: {
            type: 'buckets',
            id: '03f8d8d34d7e5000',
            orgID: '03f4f29fd0011000',
            name: 'A',
            org: "Chris' Org",
          },
        },
        {
          action: 'read',
          resource: {
            type: 'buckets',
            id: '043e71cf1922d000',
            orgID: '03f4f29fd0011000',
            name: 'B',
            org: "Chris' Org",
          },
        },
      ],
    } as Authorization

    const writeCBucketToken = {
      id: '043f7799e2ad3000',
      token: 'c',
      status: 'active',
      description: 'scoped write token (to C)',
      orgID: '03f4f29fd0011000',
      org: "Chris' Org",
      userID: '03f4f29fbd811000',
      user: 'chris',
      permissions: [
        {
          action: 'write',
          resource: {
            type: 'buckets',
            id: '03f4f29fd0011001',
            orgID: '03f4f29fd0011000',
            name: 'C',
            org: "Chris' Org",
          },
        },
      ],
    } as Authorization

    const tokens = [allAccessToken, readAAndBBucketsToken, writeCBucketToken]

    expect(filterIrrelevantAuths(tokens, 'read', ['A', 'B', 'C'])).toEqual([
      allAccessToken,
    ])

    expect(filterIrrelevantAuths(tokens, 'read', ['B', 'C'])).toEqual([
      allAccessToken,
    ])

    expect(filterIrrelevantAuths(tokens, 'read', ['A', 'B'])).toEqual([
      allAccessToken,
      readAAndBBucketsToken,
    ])

    expect(filterIrrelevantAuths(tokens, 'read', ['C'])).toEqual([
      allAccessToken,
    ])

    expect(filterIrrelevantAuths(tokens, 'write', ['C'])).toEqual([
      allAccessToken,
      writeCBucketToken,
    ])

    expect(filterIrrelevantAuths(tokens, 'write', ['A'])).toEqual([
      allAccessToken,
    ])
  })
})
