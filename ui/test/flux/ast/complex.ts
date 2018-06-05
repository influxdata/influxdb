export default {
  type: 'Program',
  location: {
    start: {
      line: 1,
      column: 1,
    },
    end: {
      line: 1,
      column: 91,
    },
    source:
      'from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "cpu") |> range(start: -1m)',
  },
  body: [
    {
      type: 'ExpressionStatement',
      location: {
        start: {
          line: 1,
          column: 1,
        },
        end: {
          line: 1,
          column: 91,
        },
        source:
          'from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "cpu") |> range(start: -1m)',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {
            line: 1,
            column: 71,
          },
          end: {
            line: 1,
            column: 91,
          },
          source: '|> range(start: -1m)',
        },
        argument: {
          type: 'PipeExpression',
          location: {
            start: {
              line: 1,
              column: 22,
            },
            end: {
              line: 1,
              column: 70,
            },
            source: '|> filter(fn: (r) => r["_measurement"] == "cpu")',
          },
          argument: {
            type: 'CallExpression',
            location: {
              start: {
                line: 1,
                column: 1,
              },
              end: {
                line: 1,
                column: 21,
              },
              source: 'from(db: "telegraf")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {
                  line: 1,
                  column: 1,
                },
                end: {
                  line: 1,
                  column: 5,
                },
                source: 'from',
              },
              name: 'from',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {
                    line: 1,
                    column: 6,
                  },
                  end: {
                    line: 1,
                    column: 20,
                  },
                  source: 'db: "telegraf"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {
                        line: 1,
                        column: 6,
                      },
                      end: {
                        line: 1,
                        column: 20,
                      },
                      source: 'db: "telegraf"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {
                          line: 1,
                          column: 6,
                        },
                        end: {
                          line: 1,
                          column: 8,
                        },
                        source: 'db',
                      },
                      name: 'db',
                    },
                    value: {
                      type: 'StringLiteral',
                      location: {
                        start: {
                          line: 1,
                          column: 10,
                        },
                        end: {
                          line: 1,
                          column: 20,
                        },
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
              start: {
                line: 1,
                column: 25,
              },
              end: {
                line: 1,
                column: 70,
              },
              source: 'filter(fn: (r) => r["_measurement"] == "cpu")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {
                  line: 1,
                  column: 25,
                },
                end: {
                  line: 1,
                  column: 31,
                },
                source: 'filter',
              },
              name: 'filter',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {
                    line: 1,
                    column: 32,
                  },
                  end: {
                    line: 1,
                    column: 69,
                  },
                  source: 'fn: (r) => r["_measurement"] == "cpu"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {
                        line: 1,
                        column: 32,
                      },
                      end: {
                        line: 1,
                        column: 69,
                      },
                      source: 'fn: (r) => r["_measurement"] == "cpu"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {
                          line: 1,
                          column: 32,
                        },
                        end: {
                          line: 1,
                          column: 34,
                        },
                        source: 'fn',
                      },
                      name: 'fn',
                    },
                    value: {
                      type: 'ArrowFunctionExpression',
                      location: {
                        start: {
                          line: 1,
                          column: 36,
                        },
                        end: {
                          line: 1,
                          column: 69,
                        },
                        source: '(r) => r["_measurement"] == "cpu"',
                      },
                      params: [
                        {
                          type: 'Property',
                          location: {
                            start: {
                              line: 1,
                              column: 37,
                            },
                            end: {
                              line: 1,
                              column: 38,
                            },
                            source: 'r',
                          },
                          key: {
                            type: 'Identifier',
                            location: {
                              start: {
                                line: 1,
                                column: 37,
                              },
                              end: {
                                line: 1,
                                column: 38,
                              },
                              source: 'r',
                            },
                            name: 'r',
                          },
                          value: null,
                        },
                      ],
                      body: {
                        type: 'BinaryExpression',
                        location: {
                          start: {
                            line: 1,
                            column: 43,
                          },
                          end: {
                            line: 1,
                            column: 69,
                          },
                          source: 'r["_measurement"] == "cpu"',
                        },
                        operator: '==',
                        left: {
                          type: 'MemberExpression',
                          location: {
                            start: {
                              line: 1,
                              column: 43,
                            },
                            end: {
                              line: 1,
                              column: 61,
                            },
                            source: 'r["_measurement"] ',
                          },
                          object: {
                            type: 'Identifier',
                            location: {
                              start: {
                                line: 1,
                                column: 43,
                              },
                              end: {
                                line: 1,
                                column: 44,
                              },
                              source: 'r',
                            },
                            name: 'r',
                          },
                          property: {
                            type: 'StringLiteral',
                            location: {
                              start: {
                                line: 1,
                                column: 45,
                              },
                              end: {
                                line: 1,
                                column: 59,
                              },
                              source: '"_measurement"',
                            },
                            value: '_measurement',
                          },
                        },
                        right: {
                          type: 'StringLiteral',
                          location: {
                            start: {
                              line: 1,
                              column: 64,
                            },
                            end: {
                              line: 1,
                              column: 69,
                            },
                            source: '"cpu"',
                          },
                          value: 'cpu',
                        },
                      },
                    },
                  },
                ],
              },
            ],
          },
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {
              line: 1,
              column: 74,
            },
            end: {
              line: 1,
              column: 91,
            },
            source: 'range(start: -1m)',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {
                line: 1,
                column: 74,
              },
              end: {
                line: 1,
                column: 79,
              },
              source: 'range',
            },
            name: 'range',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              location: {
                start: {
                  line: 1,
                  column: 80,
                },
                end: {
                  line: 1,
                  column: 90,
                },
                source: 'start: -1m',
              },
              properties: [
                {
                  type: 'Property',
                  location: {
                    start: {
                      line: 1,
                      column: 80,
                    },
                    end: {
                      line: 1,
                      column: 90,
                    },
                    source: 'start: -1m',
                  },
                  key: {
                    type: 'Identifier',
                    location: {
                      start: {
                        line: 1,
                        column: 80,
                      },
                      end: {
                        line: 1,
                        column: 85,
                      },
                      source: 'start',
                    },
                    name: 'start',
                  },
                  value: {
                    type: 'UnaryExpression',
                    location: {
                      start: {
                        line: 1,
                        column: 87,
                      },
                      end: {
                        line: 1,
                        column: 90,
                      },
                      source: '-1m',
                    },
                    operator: '-',
                    argument: {
                      type: 'DurationLiteral',
                      location: {
                        start: {
                          line: 1,
                          column: 88,
                        },
                        end: {
                          line: 1,
                          column: 90,
                        },
                        source: '1m',
                      },
                      value: '1m0s',
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
