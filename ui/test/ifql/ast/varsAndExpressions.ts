export default {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 129},
    source:
      'literal = "foo"\n\ntele = from(db: "telegraf")\n\t|\u003e range(start: -15m)\n\nfrom(db: "telegraf")\n\t|\u003e filter() \n\t|\u003e range(start: -15m)\n\n',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 16},
        source: 'literal = "foo"',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 8},
              source: 'literal',
            },
            name: 'literal',
          },
          init: {
            type: 'StringLiteral',
            location: {
              start: {line: 1, column: 11},
              end: {line: 1, column: 16},
              source: '"foo"',
            },
            value: 'foo',
          },
        },
      ],
    },
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 3, column: 1},
        end: {line: 3, column: 53},
        source: 'tele = from(db: "telegraf")\n\t|\u003e range(start: -15m)\n\n',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 3, column: 1},
              end: {line: 3, column: 5},
              source: 'tele',
            },
            name: 'tele',
          },
          init: {
            type: 'PipeExpression',
            location: {
              start: {line: 4, column: 2},
              end: {line: 4, column: 23},
              source: '|\u003e range(start: -15m)',
            },
            argument: {
              type: 'CallExpression',
              location: {
                start: {line: 3, column: 8},
                end: {line: 3, column: 28},
                source: 'from(db: "telegraf")',
              },
              callee: {
                type: 'Identifier',
                location: {
                  start: {line: 3, column: 8},
                  end: {line: 3, column: 12},
                  source: 'from',
                },
                name: 'from',
              },
              arguments: [
                {
                  type: 'ObjectExpression',
                  location: {
                    start: {line: 3, column: 13},
                    end: {line: 3, column: 27},
                    source: 'db: "telegraf"',
                  },
                  properties: [
                    {
                      type: 'Property',
                      location: {
                        start: {line: 3, column: 13},
                        end: {line: 3, column: 27},
                        source: 'db: "telegraf"',
                      },
                      key: {
                        type: 'Identifier',
                        location: {
                          start: {line: 3, column: 13},
                          end: {line: 3, column: 15},
                          source: 'db',
                        },
                        name: 'db',
                      },
                      value: {
                        type: 'StringLiteral',
                        location: {
                          start: {line: 3, column: 17},
                          end: {line: 3, column: 27},
                          source: '"telegraf"',
                        },
                        value: 'telegraf',
                      },
                    },
                  ],
                },
              ],
            },
            call: {
              type: 'CallExpression',
              location: {
                start: {line: 4, column: 5},
                end: {line: 4, column: 23},
                source: 'range(start: -15m)',
              },
              callee: {
                type: 'Identifier',
                location: {
                  start: {line: 4, column: 5},
                  end: {line: 4, column: 10},
                  source: 'range',
                },
                name: 'range',
              },
              arguments: [
                {
                  type: 'ObjectExpression',
                  location: {
                    start: {line: 4, column: 11},
                    end: {line: 4, column: 22},
                    source: 'start: -15m',
                  },
                  properties: [
                    {
                      type: 'Property',
                      location: {
                        start: {line: 4, column: 11},
                        end: {line: 4, column: 22},
                        source: 'start: -15m',
                      },
                      key: {
                        type: 'Identifier',
                        location: {
                          start: {line: 4, column: 11},
                          end: {line: 4, column: 16},
                          source: 'start',
                        },
                        name: 'start',
                      },
                      value: {
                        type: 'UnaryExpression',
                        location: {
                          start: {line: 4, column: 18},
                          end: {line: 4, column: 22},
                          source: '-15m',
                        },
                        operator: '-',
                        argument: {
                          type: 'DurationLiteral',
                          location: {
                            start: {line: 4, column: 19},
                            end: {line: 4, column: 22},
                            source: '15m',
                          },
                          value: '15m0s',
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
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 6, column: 1},
        end: {line: 6, column: 60},
        source:
          'from(db: "telegraf")\n\t|\u003e filter() \n\t|\u003e range(start: -15m)\n\n',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 8, column: 2},
          end: {line: 8, column: 23},
          source: '|\u003e range(start: -15m)',
        },
        argument: {
          type: 'PipeExpression',
          location: {
            start: {line: 7, column: 2},
            end: {line: 7, column: 13},
            source: '|\u003e filter()',
          },
          argument: {
            type: 'CallExpression',
            location: {
              start: {line: 6, column: 1},
              end: {line: 6, column: 21},
              source: 'from(db: "telegraf")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {line: 6, column: 1},
                end: {line: 6, column: 5},
                source: 'from',
              },
              name: 'from',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {line: 6, column: 6},
                  end: {line: 6, column: 20},
                  source: 'db: "telegraf"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {line: 6, column: 6},
                      end: {line: 6, column: 20},
                      source: 'db: "telegraf"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {line: 6, column: 6},
                        end: {line: 6, column: 8},
                        source: 'db',
                      },
                      name: 'db',
                    },
                    value: {
                      type: 'StringLiteral',
                      location: {
                        start: {line: 6, column: 10},
                        end: {line: 6, column: 20},
                        source: '"telegraf"',
                      },
                      value: 'telegraf',
                    },
                  },
                ],
              },
            ],
          },
          call: {
            type: 'CallExpression',
            location: {
              start: {line: 7, column: 5},
              end: {line: 7, column: 13},
              source: 'filter()',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {line: 7, column: 5},
                end: {line: 7, column: 11},
                source: 'filter',
              },
              name: 'filter',
            },
          },
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 8, column: 5},
            end: {line: 8, column: 23},
            source: 'range(start: -15m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 8, column: 5},
              end: {line: 8, column: 10},
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {line: 8, column: 11},
                end: {line: 8, column: 22},
                source: 'start: -15m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {line: 8, column: 11},
                    end: {line: 8, column: 22},
                    source: 'start: -15m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {line: 8, column: 11},
                      end: {line: 8, column: 16},
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {line: 8, column: 18},
                      end: {line: 8, column: 22},
                      source: '-15m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {line: 8, column: 19},
                        end: {line: 8, column: 22},
                        source: '15m',
                      },
                      value: '15m0s',
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
