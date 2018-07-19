import buildInfluxQLQuery, {buildQuery} from 'src/utils/influxql'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'

import {NONE, NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {TYPE_QUERY_CONFIG} from 'src/dashboards/constants'

import {QueryConfig} from 'src/types'

function mergeConfig(options: Partial<QueryConfig>) {
  return {...defaultQueryConfig({id: '123'}), ...options}
}

describe('buildInfluxQLQuery', () => {
  let config
  let timeBounds
  describe('when information is missing', () => {
    it('returns a null select statement', () => {
      timeBounds = {lower: 'now() - 15m', upper: null}
      expect(buildInfluxQLQuery(timeBounds, mergeConfig({}))).toBe(null)

      let merged = mergeConfig({database: 'db1'})
      let actual = buildInfluxQLQuery(timeBounds, merged)
      expect(actual).toBe(null) // no measurement

      merged = mergeConfig({database: 'db1', measurement: 'm1'})
      actual = buildInfluxQLQuery(timeBounds, merged)
      expect(actual).toBe(null) // no fields
    })
  })

  describe('with a database, measurement, field, and NO retention policy', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm1',
        fields: [{value: 'f1', type: 'field'}],
      })
      timeBounds = {lower: null, upper: null}
    })

    it('builds the right query', () => {
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(
        'SELECT "f1" FROM "db1".."m1"'
      )
    })
  })

  describe('with a database, measurement, retention policy, and field', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm1',
        retentionPolicy: 'rp1',
        fields: [{value: 'f1', type: 'field'}],
      })
    })

    it('builds the right query', () => {
      const actual = buildInfluxQLQuery(timeBounds, config)
      const expected = 'SELECT "f1" FROM "db1"."rp1"."m1"'
      expect(actual).toBe(expected)
    })

    it('builds the right query with a time range', () => {
      timeBounds = {lower: 'now() - 1hr', upper: null}
      const actual = buildInfluxQLQuery(timeBounds, config)
      const expected =
        'SELECT "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 1hr'

      expect(actual).toBe(expected)
    })
  })

  describe('when the field is *', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm1',
        retentionPolicy: 'rp1',
        fields: [{value: '*', type: 'field'}],
      })
      timeBounds = {lower: null, upper: null}
    })

    it('does not quote the star', () => {
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(
        'SELECT * FROM "db1"."rp1"."m1"'
      )
    })
  })

  describe('with a measurement and one field, an aggregate, and a GROUP BY time()', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm0',
        retentionPolicy: 'rp1',
        fields: [
          {
            value: 'min',
            type: 'func',
            alias: 'min_value',
            args: [{value: 'value', type: 'field'}],
          },
        ],
        groupBy: {time: '10m', tags: []},
        fill: NULL_STRING,
      })
      timeBounds = {lower: 'now() - 12h'}
    })

    it('builds the right query', () => {
      const expected =
        'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m) FILL(null)'
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
    })
  })

  describe('with a measurement and one field, an aggregate, and a GROUP BY tags', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm0',
        retentionPolicy: 'rp1',
        fields: [
          {
            value: 'min',
            type: 'func',
            alias: 'min_value',
            args: [{value: 'value', type: 'field'}],
          },
        ],
        groupBy: {time: null, tags: ['t1', 't2']},
      })
      timeBounds = {lower: 'now() - 12h'}
    })

    it('builds the right query', () => {
      const expected = `SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY "t1", "t2"`
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
    })
  })

  describe('with a measurement, one field, and an upper / lower absolute time range', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        retentionPolicy: 'rp1',
        measurement: 'm0',
        fields: [{value: 'value', type: 'field'}],
      })
      timeBounds = {
        lower: "'2015-07-23T15:52:24.447Z'",
        upper: "'2015-07-24T15:52:24.447Z'",
      }
    })

    it('builds the right query', () => {
      const expected =
        'SELECT "value" FROM "db1"."rp1"."m0" WHERE time > \'2015-07-23T15:52:24.447Z\' AND time < \'2015-07-24T15:52:24.447Z\''
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
    })
  })

  describe('with a measurement and one field, an aggregate, and a GROUP BY time(), and tags', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        retentionPolicy: 'rp1',
        measurement: 'm0',
        fields: [
          {
            value: 'min',
            type: 'func',
            alias: 'min_value',
            args: [{value: 'value', type: 'field'}],
          },
        ],
        groupBy: {time: '10m', tags: ['t1', 't2']},
        fill: NULL_STRING,
      })
      timeBounds = {lower: 'now() - 12h'}
    })

    it('builds the right query', () => {
      const expected =
        'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m), "t1", "t2" FILL(null)'
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
    })
  })

  describe('with a measurement and two fields', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        retentionPolicy: 'rp1',
        measurement: 'm0',
        fields: [{value: 'f0', type: 'field'}, {value: 'f1', type: 'field'}],
      })
      timeBounds = {upper: "'2015-02-24T00:00:00Z'"}
    })

    it('builds the right query', () => {
      timeBounds = {lower: null, upper: null}
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(
        'SELECT "f0", "f1" FROM "db1"."rp1"."m0"'
      )
    })

    it('builds the right query with a time range', () => {
      const expected = `SELECT "f0", "f1" FROM "db1"."rp1"."m0" WHERE time < '2015-02-24T00:00:00Z'`
      expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
    })

    describe('with multiple tag pairs', () => {
      beforeEach(() => {
        config = mergeConfig({
          database: 'db1',
          measurement: 'm0',
          retentionPolicy: 'rp1',
          fields: [{value: 'f0', type: 'field'}],
          tags: {
            k1: ['v1', 'v3', 'v4'],
            k2: ['v2'],
          },
        })
        timeBounds = {lower: 'now() - 6h'}
      })

      it('correctly uses AND/OR to combine pairs', () => {
        const expected = `SELECT "f0" FROM "db1"."rp1"."m0" WHERE time > now() - 6h AND ("k1"='v1' OR "k1"='v3' OR "k1"='v4') AND "k2"='v2'`
        expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
      })
    })
  })

  describe('with GROUP BY time()', () => {
    describe('and no explicit fill', () => {
      it('makes fill(null) explicit', () => {
        config = mergeConfig({
          database: 'db1',
          retentionPolicy: 'rp1',
          measurement: 'm0',
          fields: [
            {
              value: 'min',
              type: 'func',
              alias: 'min_value',
              args: [{value: 'value', type: 'field'}],
            },
          ],
          groupBy: {time: '10m', tags: []},
        })
        timeBounds = {lower: 'now() - 12h'}

        const expected =
          'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m) FILL(null)'
        expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
      })
    })

    describe('and explicit fills', () => {
      it('includes those explicit fills', () => {
        // Test fill null
        config = mergeConfig({
          database: 'db1',
          retentionPolicy: 'rp1',
          measurement: 'm0',
          fields: [
            {
              value: 'min',
              type: 'func',
              alias: 'min_value',
              args: [{value: 'value', type: 'field'}],
            },
          ],
          groupBy: {time: '10m', tags: []},
          fill: NULL_STRING,
        })
        timeBounds = {lower: 'now() - 12h'}

        let expected =
          'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m) FILL(null)'
        expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)

        // Test fill another option
        config = mergeConfig({
          database: 'db1',
          retentionPolicy: 'rp1',
          measurement: 'm0',
          fields: [
            {
              value: 'min',
              type: 'func',
              alias: 'min_value',
              args: [{value: 'value', type: 'field'}],
            },
          ],
          groupBy: {time: '10m', tags: []},
          fill: NONE,
        })
        timeBounds = {lower: 'now() - 12h'}

        expected =
          'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m) FILL(none)'
        expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)

        // Test fill number
        config = mergeConfig({
          database: 'db1',
          retentionPolicy: 'rp1',
          measurement: 'm0',
          fields: [
            {
              value: 'min',
              type: 'func',
              alias: 'min_value',
              args: [{value: 'value', type: 'field'}],
            },
          ],
          groupBy: {time: '10m', tags: ['t1', 't2']},
          fill: '1337',
        })
        timeBounds = {lower: 'now() - 12h'}

        expected =
          'SELECT min("value") AS "min_value" FROM "db1"."rp1"."m0" WHERE time > now() - 12h GROUP BY time(10m), "t1", "t2" FILL(1337)'
        expect(buildInfluxQLQuery(timeBounds, config)).toBe(expected)
      })
    })
  })

  describe('build query', () => {
    beforeEach(() => {
      config = mergeConfig({
        database: 'db1',
        measurement: 'm1',
        retentionPolicy: 'rp1',
        fields: [{value: 'f1', type: 'field'}],
        groupBy: {time: '10m', tags: []},
      })
    })

    it('builds an influxql relative time bound query', () => {
      const timeRange = {upper: null, lower: 'now() - 15m'}
      const expected =
        'SELECT "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 15m GROUP BY time(10m) FILL(null)'
      const actual = buildQuery(TYPE_QUERY_CONFIG, timeRange, config)

      expect(actual).toBe(expected)
    })
  })
})
