import timeSeriesToDygraph from 'src/utils/timeSeriesToDygraph';

describe('timeSeriesToDygraph', () => {
  it('parses a raw InfluxDB response into a dygraph friendly data format', () => {
    const influxResponse = [
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"m1",
                  "columns": ["time","f1"],
                  "values": [[1000, 1],[2000, 2]],
                },
              ]
            },
            {
              "series": [
                {
                  "name":"m1",
                  "columns": ["time","f2"],
                  "values": [[2000, 3],[4000, 4]],
                },
              ]
            },
          ],
        },
      }
    ];

    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      fields: [
        'time',
        `m1.f1`,
        `m1.f2`,
      ],
      timeSeries: [
        [new Date(1000), 1, null],
        [new Date(2000), 2, 3],
        [new Date(4000), null, 4],
      ],
    };

    expect(actual).to.deep.equal(expected);
  });

  it('can sort numerical timestamps correctly', () => {
    const influxResponse = [
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"m1",
                  "columns": ["time","f1"],
                  "values": [[100, 1],[3000, 3],[200, 2]],
                },
              ]
            },
          ],
        },
      }
    ];


    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      fields: [
        'time',
        'm1.f1',
      ],
      timeSeries: [
        [new Date(100), 1],
        [new Date(200), 2],
        [new Date(3000), 3],
      ],
    };

    expect(actual).to.deep.equal(expected);
  });
});
