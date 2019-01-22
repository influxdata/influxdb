import {getMinDurationFromAST} from 'src/shared/utils/getMinDurationFromAST'

const AST_1 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 39},
    source: 'from(bucket: "b") |\u003e range(start: -1m)',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 39},
        source: 'from(bucket: "b") |\u003e range(start: -1m)',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 39},
          source: '|\u003e range(start: -1m)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 39},
            source: 'range(start: -1m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 38},
                source: 'start: -1m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 38},
                    source: 'start: -1m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 38},
                      source: '-1m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 36},
                        end: {line: 1, column: 38},
                        source: '1m',
                      },
                      values: [{magnitude: 1, unit: 'm'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_2 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 50},
    source: 'foo = -1m\n\nfrom(bucket: "b") |\u003e range(start: foo)',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 12},
        source: 'foo = -1m\n\n',
      },
      declarations: [
        {
          type: 'VariableAssignment',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 4},
              source: 'foo',
            },
            name: 'foo',
          },
          init: {
            type: 'UnaryExpression',
            location: {
              start: {line: 1, column: 7},
              end: {line: 1, column: 12},
              source: '-1m\n\n',
            },
            operator: '-',
            argument: {
              type: 'DurationLiteral',
              location: {
                start: {line: 1, column: 8},
                end: {line: 1, column: 10},
                source: '1m',
              },
              values: [{magnitude: 1, unit: 'm'}],
            },
          },
        },
      ],
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 3, column: 1},
        end: {line: 3, column: 39},
        source: 'from(bucket: "b") |\u003e range(start: foo)',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 3, column: 19},
          end: {line: 3, column: 39},
          source: '|\u003e range(start: foo)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 3, column: 1},
            end: {line: 3, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 3, column: 1},
              end: {line: 3, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 3, column: 6},
                end: {line: 3, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 3, column: 6},
                    end: {line: 3, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 3, column: 6},
                      end: {line: 3, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 3, column: 14},
                      end: {line: 3, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 3, column: 22},
            end: {line: 3, column: 39},
            source: 'range(start: foo)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 3, column: 22},
              end: {line: 3, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 3, column: 28},
                end: {line: 3, column: 38},
                source: 'start: foo',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 3, column: 28},
                    end: {line: 3, column: 38},
                    source: 'start: foo',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 3, column: 28},
                      end: {line: 3, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'Identifier',
                    location: {
                      start: {line: 3, column: 35},
                      end: {line: 3, column: 38},
                      source: 'foo',
                    },
                    name: 'foo',
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_3 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 50},
    source: 'from(bucket: "b") |\u003e range(start: -1h, stop: -3m)',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 50},
        source: 'from(bucket: "b") |\u003e range(start: -1h, stop: -3m)',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 50},
          source: '|\u003e range(start: -1h, stop: -3m)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 50},
            source: 'range(start: -1h, stop: -3m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 49},
                source: 'start: -1h, stop: -3m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 38},
                    source: 'start: -1h',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 38},
                      source: '-1h',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 36},
                        end: {line: 1, column: 38},
                        source: '1h',
                      },
                      values: [{magnitude: 1, unit: 'h'}],
                    },
                  },
                },
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 40},
                    end: {line: 1, column: 49},
                    source: 'stop: -3m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 40},
                      end: {line: 1, column: 44},
                      source: 'stop',
                    },
                    name: 'stop',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 46},
                      end: {line: 1, column: 49},
                      source: '-3m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 47},
                        end: {line: 1, column: 49},
                        source: '3m',
                      },
                      values: [{magnitude: 3, unit: 'm'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_4 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 85},
    source:
      'from(bucket: "b") |\u003e range(start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z) ',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 85},
        source:
          'from(bucket: "b") |\u003e range(start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z) ',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 84},
          source:
            '|\u003e range(start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 84},
            source:
              'range(start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 83},
                source:
                  'start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 55},
                    source: 'start: 2010-01-01T00:00:00Z',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'DateTimeLiteral',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 55},
                      source: '2010-01-01T00:00:00Z',
                    },
                    value: '2010-01-01T00:00:00Z',
                  },
                },
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 57},
                    end: {line: 1, column: 83},
                    source: 'stop: 2010-01-02T00:00:00Z',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 57},
                      end: {line: 1, column: 61},
                      source: 'stop',
                    },
                    name: 'stop',
                  },
                  value: {
                    type: 'DateTimeLiteral',
                    location: {
                      start: {line: 1, column: 63},
                      end: {line: 1, column: 83},
                      source: '2010-01-02T00:00:00Z',
                    },
                    value: '2010-01-02T00:00:00Z',
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_5 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 91},
    source:
      'from(bucket: "b") |\u003e range(start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z)  ',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 91},
        source:
          'from(bucket: "b") |\u003e range(start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z)  ',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 89},
          source:
            '|\u003e range(start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 89},
            source:
              'range(start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 88},
                source:
                  'start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 60},
                    source: 'start: 2010-01-01T00:00:00Z + 1h',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'BinaryExpression',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 60},
                      source: '2010-01-01T00:00:00Z + 1h',
                    },
                    operator: '+',
                    left: {
                      type: 'DateTimeLiteral',
                      location: {
                        start: {line: 1, column: 35},
                        end: {line: 1, column: 55},
                        source: '2010-01-01T00:00:00Z',
                      },
                      value: '2010-01-01T00:00:00Z',
                    },
                    right: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 58},
                        end: {line: 1, column: 60},
                        source: '1h',
                      },
                      values: [{magnitude: 1, unit: 'h'}],
                    },
                  },
                },
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 62},
                    end: {line: 1, column: 88},
                    source: 'stop: 2010-01-02T00:00:00Z',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 62},
                      end: {line: 1, column: 66},
                      source: 'stop',
                    },
                    name: 'stop',
                  },
                  value: {
                    type: 'DateTimeLiteral',
                    location: {
                      start: {line: 1, column: 68},
                      end: {line: 1, column: 88},
                      source: '2010-01-02T00:00:00Z',
                    },
                    value: '2010-01-02T00:00:00Z',
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_6 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 78},
    source:
      'from(bucket: "b") |\u003e range(start: -2m)\nfrom(bucket: "b") |\u003e range(start: -1m)',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 40},
        source: 'from(bucket: "b") |\u003e range(start: -2m)\n',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 39},
          source: '|\u003e range(start: -2m)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 39},
            source: 'range(start: -2m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 38},
                source: 'start: -2m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 38},
                    source: 'start: -2m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 38},
                      source: '-2m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 36},
                        end: {line: 1, column: 38},
                        source: '2m',
                      },
                      values: [{magnitude: 2, unit: 'm'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 2, column: 1},
        end: {line: 2, column: 39},
        source: 'from(bucket: "b") |\u003e range(start: -1m)',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 2, column: 19},
          end: {line: 2, column: 39},
          source: '|\u003e range(start: -1m)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 2, column: 1},
            end: {line: 2, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 1},
              end: {line: 2, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 2, column: 6},
                end: {line: 2, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 2, column: 6},
                    end: {line: 2, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 2, column: 6},
                      end: {line: 2, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 2, column: 14},
                      end: {line: 2, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 2, column: 22},
            end: {line: 2, column: 39},
            source: 'range(start: -1m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 22},
              end: {line: 2, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 2, column: 28},
                end: {line: 2, column: 38},
                source: 'start: -1m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 2, column: 28},
                    end: {line: 2, column: 38},
                    source: 'start: -1m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 2, column: 28},
                      end: {line: 2, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 2, column: 35},
                      end: {line: 2, column: 38},
                      source: '-1m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 2, column: 36},
                        end: {line: 2, column: 38},
                        source: '1m',
                      },
                      values: [{magnitude: 1, unit: 'm'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_7 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 96},
    source:
      'from(bucket: "b") |\u003e range(start: -200d, stop: -100d)\nfrom(bucket: "b") |\u003e range(start: -101d) ',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 55},
        source: 'from(bucket: "b") |\u003e range(start: -200d, stop: -100d)\n',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 1, column: 19},
          end: {line: 1, column: 54},
          source: '|\u003e range(start: -200d, stop: -100d)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 6},
                end: {line: 1, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 6},
                    end: {line: 1, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 6},
                      end: {line: 1, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 1, column: 14},
                      end: {line: 1, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 1, column: 22},
            end: {line: 1, column: 54},
            source: 'range(start: -200d, stop: -100d)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 22},
              end: {line: 1, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 1, column: 28},
                end: {line: 1, column: 53},
                source: 'start: -200d, stop: -100d',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 28},
                    end: {line: 1, column: 40},
                    source: 'start: -200d',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 28},
                      end: {line: 1, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 35},
                      end: {line: 1, column: 40},
                      source: '-200d',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 36},
                        end: {line: 1, column: 40},
                        source: '200d',
                      },
                      values: [{magnitude: 200, unit: 'd'}],
                    },
                  },
                },
                {
                  type: 'Property',
                  location: {
                    start: {line: 1, column: 42},
                    end: {line: 1, column: 53},
                    source: 'stop: -100d',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 1, column: 42},
                      end: {line: 1, column: 46},
                      source: 'stop',
                    },
                    name: 'stop',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 1, column: 48},
                      end: {line: 1, column: 53},
                      source: '-100d',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 1, column: 49},
                        end: {line: 1, column: 53},
                        source: '100d',
                      },
                      values: [{magnitude: 100, unit: 'd'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 2, column: 1},
        end: {line: 2, column: 42},
        source: 'from(bucket: "b") |\u003e range(start: -101d) ',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 2, column: 19},
          end: {line: 2, column: 41},
          source: '|\u003e range(start: -101d)',
        },
        argument: {
          type: 'CallExpression',
          location: {
            start: {line: 2, column: 1},
            end: {line: 2, column: 18},
            source: 'from(bucket: "b")',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 1},
              end: {line: 2, column: 5},
              source: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 2, column: 6},
                end: {line: 2, column: 17},
                source: 'bucket: "b"',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 2, column: 6},
                    end: {line: 2, column: 17},
                    source: 'bucket: "b"',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 2, column: 6},
                      end: {line: 2, column: 12},
                      source: 'bucket',
                    },
                    name: 'bucket',
                  },
                  value: {
                    type: 'StringLiteral',
                    location: {
                      start: {line: 2, column: 14},
                      end: {line: 2, column: 17},
                      source: '"b"',
                    },
                    value: 'b',
                  },
                },
              ],
            },
          ],
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 2, column: 22},
            end: {line: 2, column: 41},
            source: 'range(start: -101d)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 22},
              end: {line: 2, column: 27},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 2, column: 28},
                end: {line: 2, column: 40},
                source: 'start: -101d',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 2, column: 28},
                    end: {line: 2, column: 40},
                    source: 'start: -101d',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 2, column: 28},
                      end: {line: 2, column: 33},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 2, column: 35},
                      end: {line: 2, column: 40},
                      source: '-101d',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 2, column: 36},
                        end: {line: 2, column: 40},
                        source: '101d',
                      },
                      values: [{magnitude: 101, unit: 'd'}],
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    },
  ],
}

const AST_8 = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 78},
    source:
      'range(start: -3s, stop: -2s)\nrange(start: -2s, stop: -1s)\nrange(start: -1s)  ',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 29},
        source: 'range(start: -3s, stop: -2s)',
      },
      expression: {
        type: 'CallExpression',
        location: {
          start: {line: 1, column: 1},
          end: {line: 1, column: 29},
          source: 'range(start: -3s, stop: -2s)',
        },
        callee: {
          type: 'Identifier',
          location: {
            start: {line: 1, column: 1},
            end: {line: 1, column: 6},
            source: 'range',
          },
          name: 'range',
        },
        arguments: [
          {
            type: 'ObjectExpression',
            location: {
              start: {line: 1, column: 7},
              end: {line: 1, column: 28},
              source: 'start: -3s, stop: -2s',
            },
            properties: [
              {
                type: 'Property',
                location: {
                  start: {line: 1, column: 7},
                  end: {line: 1, column: 17},
                  source: 'start: -3s',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 1, column: 7},
                    end: {line: 1, column: 12},
                    source: 'start',
                  },
                  name: 'start',
                },
                value: {
                  type: 'UnaryExpression',
                  location: {
                    start: {line: 1, column: 14},
                    end: {line: 1, column: 17},
                    source: '-3s',
                  },
                  operator: '-',
                  argument: {
                    type: 'DurationLiteral',
                    location: {
                      start: {line: 1, column: 15},
                      end: {line: 1, column: 17},
                      source: '3s',
                    },
                    values: [{magnitude: 3, unit: 's'}],
                  },
                },
              },
              {
                type: 'Property',
                location: {
                  start: {line: 1, column: 19},
                  end: {line: 1, column: 28},
                  source: 'stop: -2s',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 1, column: 19},
                    end: {line: 1, column: 23},
                    source: 'stop',
                  },
                  name: 'stop',
                },
                value: {
                  type: 'UnaryExpression',
                  location: {
                    start: {line: 1, column: 25},
                    end: {line: 1, column: 28},
                    source: '-2s',
                  },
                  operator: '-',
                  argument: {
                    type: 'DurationLiteral',
                    location: {
                      start: {line: 1, column: 26},
                      end: {line: 1, column: 28},
                      source: '2s',
                    },
                    values: [{magnitude: 2, unit: 's'}],
                  },
                },
              },
            ],
          },
        ],
      },
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 2, column: 1},
        end: {line: 2, column: 29},
        source: 'range(start: -2s, stop: -1s)',
      },
      expression: {
        type: 'CallExpression',
        location: {
          start: {line: 2, column: 1},
          end: {line: 2, column: 29},
          source: 'range(start: -2s, stop: -1s)',
        },
        callee: {
          type: 'Identifier',
          location: {
            start: {line: 2, column: 1},
            end: {line: 2, column: 6},
            source: 'range',
          },
          name: 'range',
        },
        arguments: [
          {
            type: 'ObjectExpression',
            location: {
              start: {line: 2, column: 7},
              end: {line: 2, column: 28},
              source: 'start: -2s, stop: -1s',
            },
            properties: [
              {
                type: 'Property',
                location: {
                  start: {line: 2, column: 7},
                  end: {line: 2, column: 17},
                  source: 'start: -2s',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 2, column: 7},
                    end: {line: 2, column: 12},
                    source: 'start',
                  },
                  name: 'start',
                },
                value: {
                  type: 'UnaryExpression',
                  location: {
                    start: {line: 2, column: 14},
                    end: {line: 2, column: 17},
                    source: '-2s',
                  },
                  operator: '-',
                  argument: {
                    type: 'DurationLiteral',
                    location: {
                      start: {line: 2, column: 15},
                      end: {line: 2, column: 17},
                      source: '2s',
                    },
                    values: [{magnitude: 2, unit: 's'}],
                  },
                },
              },
              {
                type: 'Property',
                location: {
                  start: {line: 2, column: 19},
                  end: {line: 2, column: 28},
                  source: 'stop: -1s',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 2, column: 19},
                    end: {line: 2, column: 23},
                    source: 'stop',
                  },
                  name: 'stop',
                },
                value: {
                  type: 'UnaryExpression',
                  location: {
                    start: {line: 2, column: 25},
                    end: {line: 2, column: 28},
                    source: '-1s',
                  },
                  operator: '-',
                  argument: {
                    type: 'DurationLiteral',
                    location: {
                      start: {line: 2, column: 26},
                      end: {line: 2, column: 28},
                      source: '1s',
                    },
                    values: [{magnitude: 1, unit: 's'}],
                  },
                },
              },
            ],
          },
        ],
      },
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 3, column: 1},
        end: {line: 3, column: 18},
        source: 'range(start: -1s)',
      },
      expression: {
        type: 'CallExpression',
        location: {
          start: {line: 3, column: 1},
          end: {line: 3, column: 18},
          source: 'range(start: -1s)',
        },
        callee: {
          type: 'Identifier',
          location: {
            start: {line: 3, column: 1},
            end: {line: 3, column: 6},
            source: 'range',
          },
          name: 'range',
        },
        arguments: [
          {
            type: 'ObjectExpression',
            location: {
              start: {line: 3, column: 7},
              end: {line: 3, column: 17},
              source: 'start: -1s',
            },
            properties: [
              {
                type: 'Property',
                location: {
                  start: {line: 3, column: 7},
                  end: {line: 3, column: 17},
                  source: 'start: -1s',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 3, column: 7},
                    end: {line: 3, column: 12},
                    source: 'start',
                  },
                  name: 'start',
                },
                value: {
                  type: 'UnaryExpression',
                  location: {
                    start: {line: 3, column: 14},
                    end: {line: 3, column: 17},
                    source: '-1s',
                  },
                  operator: '-',
                  argument: {
                    type: 'DurationLiteral',
                    location: {
                      start: {line: 3, column: 15},
                      end: {line: 3, column: 17},
                      source: '1s',
                    },
                    values: [{magnitude: 1, unit: 's'}],
                  },
                },
              },
            ],
          },
        ],
      },
    },
  ],
}

// prettier-ignore
export const AST_TESTS = [
  ['basic relative query'                               , 'from(bucket: "b") |> range(start: -1m)'                                                          , 1000 * 60                , AST_1],
  ['query with start time assigned to variable'         , 'foo = -1m\n\nfrom(bucket: "b") |> range(start: foo)'                                             , 1000 * 60                , AST_2],
  ['query with relative start and stop times'           , 'from(bucket: "b") |> range(start: -1h, stop: -3m)'                                               , 1000 * 60 * 57           , AST_3],
  ['query with absolute start and stop times'           , 'from(bucket: "b") |> range(start: 2010-01-01T00:00:00Z, stop: 2010-01-02T00:00:00Z)'             , 1000 * 60 * 60 * 24      , AST_4],
  ['query with duration literal added to absolute time' , 'from(bucket: "b") |> range(start: 2010-01-01T00:00:00Z + 1h, stop: 2010-01-02T00:00:00Z)'        , 1000 * 60 * 60 * 23      , AST_5],
  ['query with two range calls'                         , 'from(bucket: "b") |> range(start: -1m)\nfrom(bucket: "b") |> range(start: -2m) '                 , 1000 * 60                , AST_6],
  ['query with two large ranges overlapping slightly'   , 'from(bucket: "b") |> range(start: -200d, stop: -100d)\nfrom(bucket: "b") |> range(start: -101d)' , 1000 * 60 * 60 * 24      , AST_7],
  ['query with three nonoverlapping ranges'             , 'range(start: -3s, stop: -2s)\nrange(start: -2s, stop: -1s)\nrange(start: -1s)'                   , 1000                     , AST_8]
]

describe('getMinDurationFromAST', () => {
  test.each(AST_TESTS)('%s:\n\n```\n%s\n```', (__, ___, expected, ast) => {
    const actual = getMinDurationFromAST(ast)

    expect(Math.abs(actual - expected)).toBeLessThan(5)
  })
})
