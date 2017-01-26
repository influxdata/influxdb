import timeSeriesToDygraph from 'src/utils/timeSeriesToDygraph';
import {STROKE_WIDTH} from 'src/shared/constants';

const {light: strokeWidth} = STROKE_WIDTH;

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
                  "tags": {
                    tk1: "tv1",
                    tk2: "tv2",
                  },
                },
              ]
            },
            {
              "series": [
                {
                  "name":"m1",
                  "columns": ["time","f2"],
                  "values": [[2000, 3],[4000, 4]],
                  "tags": {
                    tk3: "tv3",
                  },
                },
              ]
            },
          ],
        },
      }
    ];

    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      labels: [
        'time',
        `m1.f1[tk1=tv1][tk2=tv2]`,
        `m1.f2[tk3=tv3]`,
      ],
      timeSeries: [
        [new Date(1000), 1, null],
        [new Date(2000), 2, 3],
        [new Date(4000), null, 4],
      ],
      dygraphSeries: {
        'm1.f1[tk1=tv1][tk2=tv2]': {
          axis: 'y',
          strokeWidth,
        },
        'm1.f2[tk3=tv3]': {
          axis: 'y',
          strokeWidth,
        },
      },
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
      labels: [
        'time',
        'm1.f1',
      ],
      timeSeries: [
        [new Date(100), 1],
        [new Date(200), 2],
        [new Date(3000), 3],
      ],
    };

    expect(actual.timeSeries).to.deep.equal(expected.timeSeries);
  });

  it('can parse multiple responses into two axes', () => {
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
      },
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"m3",
                  "columns": ["time","f3"],
                  "values": [[1000, 1],[2000, 2]],
                },
              ]
            },
          ],
        },
      },
    ];

    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      'm1.f1': {
        axis: 'y',
        strokeWidth,
      },
      'm1.f2': {
        axis: 'y',
        strokeWidth,
      },
      'm3.f3': {
        axis: 'y2',
        strokeWidth,
      },
    };

    expect(actual.dygraphSeries).to.deep.equal(expected);
  });

  it('can parse multiple responses with the same field and measurement', () => {
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
          ],
        },
      },
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"m1",
                  "columns": ["time","f1"],
                  "values": [[2000, 3],[4000, 4]],
                },
              ]
            },
          ],
        },
      },
    ];

    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      labels: [
        'time',
        `m1.f1`,
        `m1.f1`,
      ],
      timeSeries: [
        [new Date(1000), 1, null],
        [new Date(2000), 2, 3],
        [new Date(4000), null, 4],
      ],
      dygraphSeries: {
        'm1.f1': {
          axis: 'y',
          strokeWidth,
        },
        'm1.f1': {
          axis: 'y2',
          strokeWidth,
        },
      },
    };

    expect(actual).to.deep.equal(expected);
  });

  it('it does not use multiple axes if being used for the DataExplorer', () => {
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
          ],
        },
      },
      {
        "response":
        {
          "results": [
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
      },
    ];

    const isInDataExplorer = true;
    const actual = timeSeriesToDygraph(influxResponse, undefined, isInDataExplorer);

    const expected = {
      'm1.f1': {
        strokeWidth,
      },
      'm1.f2': {
        strokeWidth,
      },
    };

    expect(actual.dygraphSeries).to.deep.equal(expected);
  });

  it('it highlights the appropriate response', () => {
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
          ],
        },
      },
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"m2",
                  "columns": ["time","f2"],
                  "values": [[2000, 3],[4000, 4]],
                },
              ]
            },
          ],
        },
      },
    ];

    const highlightIndex = 1;
    const actual = timeSeriesToDygraph(influxResponse, highlightIndex);
    const {dygraphSeries} = actual;

    expect(dygraphSeries["m2.f2"].strokeWidth).to.be.above(dygraphSeries["m1.f1"].strokeWidth);
  });

  it('parses labels alphabetically with the correct field values for multiple series', () => {
    const influxResponse = [
      {
        "response":
        {
          "results": [
            {
              "series": [
                {
                  "name":"mb",
                  "columns": ["time","fa"],
                  "values": [
                    [1000, 200],
                    [2000, 300],
                    [4000, 400],
                  ],
                },
                {
                  "name":"mc",
                  "columns": ["time","fa"],
                  "values": [
                    [1000, 400],
                    [2000, 600],
                    [3000, 800],
                    [5000, 1000],
                  ],
                },
              ]
            },
            {
              "series": [
                {
                  "name":"ma",
                  "columns": ["time","fa","fc","fb"],
                  "values": [
                    [1000, 20, 10, 10],
                    [2000, 30, 15, 9],
                    [3000, 40, 20, 8],
                  ],
                },
              ]
            },
          ],
        },
      }
    ];

    const actual = timeSeriesToDygraph(influxResponse);

    const expected = {
      labels: [
        'time',
        `ma.fa`,
        `ma.fb`,
        `ma.fc`,
        `mb.fa`,
        `mc.fa`,
      ],
      timeSeries: [
        [new Date(1000), 20, 10, 10, 200, 400],
        [new Date(2000), 30, 9, 15, 300, 600],
        [new Date(3000), 40, 8, 20, null, 800],
        [new Date(4000), null, null, null, 400, null],
        [new Date(5000), null, null, null, null, 1000],
      ],
    };

    expect(actual.labels).to.deep.equal(expected.labels);
    expect(actual.timeSeries).to.deep.equal(expected.timeSeries);
  });
});
