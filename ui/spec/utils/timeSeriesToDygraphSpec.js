import timeSeriesToDygraph from 'src/utils/timeSeriesToDygraph'

describe('timeSeriesToDygraph', () => {
  it('parses a raw InfluxDB response into a dygraph friendly data format', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f2'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
          ],
        },
      },
    ]

    const actual = timeSeriesToDygraph(influxResponse)

    const expected = {
      labels: ['time', `m1.f1`, `m1.f2`],
      timeSeries: [
        [new Date(1000), 1, null],
        [new Date(2000), 2, 3],
        [new Date(4000), null, 4],
      ],
      dygraphSeries: {
        'm1.f1': {
          axis: 'y',
        },
        'm1.f2': {
          axis: 'y',
        },
      },
    }

    expect(actual).to.deep.equal(expected)
  })

  it('can sort numerical timestamps correctly', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[100, 1], [3000, 3], [200, 2]],
                },
              ],
            },
          ],
        },
      },
    ]

    const actual = timeSeriesToDygraph(influxResponse)

    const expected = {
      labels: ['time', 'm1.f1'],
      timeSeries: [[new Date(100), 1], [new Date(200), 2], [new Date(3000), 3]],
    }

    expect(actual.timeSeries).to.deep.equal(expected.timeSeries)
  })

  it('can parse multiple responses into two axes', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f2'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
          ],
        },
      },
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm3',
                  columns: ['time', 'f3'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
          ],
        },
      },
    ]

    const actual = timeSeriesToDygraph(influxResponse)

    const expected = {
      'm1.f1': {
        axis: 'y',
      },
      'm1.f2': {
        axis: 'y',
      },
      'm3.f3': {
        axis: 'y2',
      },
    }

    expect(actual.dygraphSeries).to.deep.equal(expected)
  })

  it('can parse multiple responses with the same field and measurement', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
          ],
        },
      },
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
          ],
        },
      },
    ]

    const actual = timeSeriesToDygraph(influxResponse)

    const expected = {
      labels: ['time', `m1.f1`, `m1.f1`],
      timeSeries: [
        [new Date(1000), 1, null],
        [new Date(2000), 2, 3],
        [new Date(4000), null, 4],
      ],
      dygraphSeries: {
        'm1.f1': {
          axis: 'y2',
        },
      },
    }

    expect(actual).to.deep.equal(expected)
  })

  it('it does not use multiple axes if being used for the DataExplorer', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
          ],
        },
      },
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'm1',
                  columns: ['time', 'f2'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
          ],
        },
      },
    ]

    const isInDataExplorer = true
    const actual = timeSeriesToDygraph(influxResponse, isInDataExplorer)

    const expected = {}

    expect(actual.dygraphSeries).to.deep.equal(expected)
  })

  it('parses a raw InfluxDB response into a dygraph friendly data format', () => {
    const influxResponse = [
      {
        response: {
          results: [
            {
              series: [
                {
                  name: 'mb',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
            {
              series: [
                {
                  name: 'ma',
                  columns: ['time', 'f1'],
                  values: [[1000, 1], [2000, 2]],
                },
              ],
            },
            {
              series: [
                {
                  name: 'mc',
                  columns: ['time', 'f2'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
            {
              series: [
                {
                  name: 'mc',
                  columns: ['time', 'f1'],
                  values: [[2000, 3], [4000, 4]],
                },
              ],
            },
          ],
        },
      },
    ]

    const actual = timeSeriesToDygraph(influxResponse)

    const expected = ['time', `ma.f1`, `mb.f1`, `mc.f1`, `mc.f2`]

    expect(actual.labels).to.deep.equal(expected)
  })
})
