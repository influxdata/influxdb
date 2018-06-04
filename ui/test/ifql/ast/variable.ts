export const StringLiteral = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 21},
    source: 'bux = "im a var"',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 21},
        source: 'bux = "im a var"',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 4},
              source: 'bux',
            },
            name: 'bux',
          },
          init: {
            type: 'StringLiteral',
            location: {
              start: {line: 1, column: 7},
              end: {line: 1, column: 21},
              source: '"im a var"',
            },
            value: 'im a var',
          },
        },
      ],
    },
  ],
}

export const Expression = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 28},
    source: 'tele = from(db: "telegraf")',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 28},
        source: 'tele = from(db: "telegraf")',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'tele',
            },
            name: 'tele',
          },
          init: {
            type: 'CallExpression',
            location: {
              start: {line: 1, column: 8},
              end: {line: 1, column: 28},
              source: 'from(db: "telegraf")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {line: 1, column: 8},
                end: {line: 1, column: 12},
                source: 'from',
              },
              name: 'from',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {line: 1, column: 13},
                  end: {line: 1, column: 27},
                  source: 'db: "telegraf"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {line: 1, column: 13},
                      end: {line: 1, column: 27},
                      source: 'db: "telegraf"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {line: 1, column: 13},
                        end: {line: 1, column: 15},
                        source: 'db',
                      },
                      name: 'db',
                    },
                    value: {
                      type: 'StringLiteral',
                      location: {
                        start: {line: 1, column: 17},
                        end: {line: 1, column: 27},
                        source: '"telegraf"',
                      },
                      value: 'telegraf',
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    },
  ],
}

export const ArrowFunction = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 22},
    source: 'addOne = (n) =\u003e n + 1',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 22},
        source: 'addOne = (n) =\u003e n + 1',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 7},
              source: 'addOne',
            },
            name: 'addOne',
          },
          init: {
            type: 'ArrowFunctionExpression',
            location: {
              start: {line: 1, column: 10},
              end: {line: 1, column: 22},
              source: '(n) =\u003e n + 1',
            },
            params: [
              {
                type: 'Property',
                location: {
                  start: {line: 1, column: 11},
                  end: {line: 1, column: 12},
                  source: 'n',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 1, column: 11},
                    end: {line: 1, column: 12},
                    source: 'n',
                  },
                  name: 'n',
                },
                value: null,
              },
            ],
            body: {
              type: 'BinaryExpression',
              location: {
                start: {line: 1, column: 17},
                end: {line: 1, column: 22},
                source: 'n + 1',
              },
              operator: '+',
              left: {
                type: 'Identifier',
                location: {
                  start: {line: 1, column: 17},
                  end: {line: 1, column: 18},
                  source: 'n',
                },
                name: 'n',
              },
              right: {
                type: 'IntegerLiteral',
                location: {
                  start: {line: 1, column: 21},
                  end: {line: 1, column: 22},
                  source: '1',
                },
                value: '1',
              },
            },
          },
        },
      ],
    },
  ],
}

export const Fork = {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 42},
    source: 'tele = from(db: "telegraf")\ntele |\u003e sum()',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 28},
        source: 'tele = from(db: "telegraf")',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 1, column: 1},
              end: {line: 1, column: 5},
              source: 'tele',
            },
            name: 'tele',
          },
          init: {
            type: 'CallExpression',
            location: {
              start: {line: 1, column: 8},
              end: {line: 1, column: 28},
              source: 'from(db: "telegraf")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {line: 1, column: 8},
                end: {line: 1, column: 12},
                source: 'from',
              },
              name: 'from',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {line: 1, column: 13},
                  end: {line: 1, column: 27},
                  source: 'db: "telegraf"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {line: 1, column: 13},
                      end: {line: 1, column: 27},
                      source: 'db: "telegraf"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {line: 1, column: 13},
                        end: {line: 1, column: 15},
                        source: 'db',
                      },
                      name: 'db',
                    },
                    value: {
                      type: 'StringLiteral',
                      location: {
                        start: {line: 1, column: 17},
                        end: {line: 1, column: 27},
                        source: '"telegraf"',
                      },
                      value: 'telegraf',
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    },
    {
      type: 'ExpressionStatement',
      location: {
        start: {line: 2, column: 1},
        end: {line: 2, column: 14},
        source: 'tele |\u003e sum()',
      },
      expression: {
        type: 'PipeExpression',
        location: {
          start: {line: 2, column: 6},
          end: {line: 2, column: 14},
          source: '|\u003e sum()',
        },
        argument: {
          type: 'Identifier',
          location: {
            start: {line: 2, column: 1},
            end: {line: 2, column: 5},
            source: 'tele',
          },
          name: 'tele',
        },
        call: {
          type: 'CallExpression',
          location: {
            start: {line: 2, column: 9},
            end: {line: 2, column: 14},
            source: 'sum()',
          },
          callee: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 9},
              end: {line: 2, column: 12},
              source: 'sum',
            },
            name: 'sum',
          },
        },
      },
    },
  ],
}

export const JoinWithObjectArg = {
  type: 'Program',
  location: {
    start: {
      line: 1,
      column: 1,
    },
    end: {
      line: 1,
      column: 106,
    },
    source:
      'join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])',
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
          column: 106,
        },
        source:
          'join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])',
      },
      expression: {
        type: 'CallExpression',
        location: {
          start: {
            line: 1,
            column: 1,
          },
          end: {
            line: 1,
            column: 106,
          },
          source:
            'join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])',
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
            source: 'join',
          },
          name: 'join',
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
                column: 105,
              },
              source:
                'tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"]',
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
                    column: 31,
                  },
                  source: 'tables:{cpu:cpu, mem:mem}',
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
                      column: 12,
                    },
                    source: 'tables',
                  },
                  name: 'tables',
                },
                value: {
                  type: 'ObjectExpression',
                  location: {
                    start: {
                      line: 1,
                      column: 14,
                    },
                    end: {
                      line: 1,
                      column: 30,
                    },
                    source: 'cpu:cpu, mem:mem',
                  },
                  properties: [
                    {
                      type: 'Property',
                      location: {
                        start: {
                          line: 1,
                          column: 14,
                        },
                        end: {
                          line: 1,
                          column: 21,
                        },
                        source: 'cpu:cpu',
                      },
                      key: {
                        type: 'Identifier',
                        location: {
                          start: {
                            line: 1,
                            column: 14,
                          },
                          end: {
                            line: 1,
                            column: 17,
                          },
                          source: 'cpu',
                        },
                        name: 'cpu',
                      },
                      value: {
                        type: 'Identifier',
                        location: {
                          start: {
                            line: 1,
                            column: 18,
                          },
                          end: {
                            line: 1,
                            column: 21,
                          },
                          source: 'cpu',
                        },
                        name: 'cpu',
                      },
                    },
                    {
                      type: 'Property',
                      location: {
                        start: {
                          line: 1,
                          column: 23,
                        },
                        end: {
                          line: 1,
                          column: 30,
                        },
                        source: 'mem:mem',
                      },
                      key: {
                        type: 'Identifier',
                        location: {
                          start: {
                            line: 1,
                            column: 23,
                          },
                          end: {
                            line: 1,
                            column: 26,
                          },
                          source: 'mem',
                        },
                        name: 'mem',
                      },
                      value: {
                        type: 'Identifier',
                        location: {
                          start: {
                            line: 1,
                            column: 27,
                          },
                          end: {
                            line: 1,
                            column: 30,
                          },
                          source: 'mem',
                        },
                        name: 'mem',
                      },
                    },
                  ],
                },
              },
              {
                type: 'Property',
                location: {
                  start: {
                    line: 1,
                    column: 33,
                  },
                  end: {
                    line: 1,
                    column: 44,
                  },
                  source: 'on:["host"]',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {
                      line: 1,
                      column: 33,
                    },
                    end: {
                      line: 1,
                      column: 35,
                    },
                    source: 'on',
                  },
                  name: 'on',
                },
                value: {
                  type: 'ArrayExpression',
                  location: {
                    start: {
                      line: 1,
                      column: 37,
                    },
                    end: {
                      line: 1,
                      column: 43,
                    },
                    source: '"host"',
                  },
                  elements: [
                    {
                      type: 'StringLiteral',
                      location: {
                        start: {
                          line: 1,
                          column: 37,
                        },
                        end: {
                          line: 1,
                          column: 43,
                        },
                        source: '"host"',
                      },
                      value: 'host',
                    },
                  ],
                },
              },
              {
                type: 'Property',
                location: {
                  start: {
                    line: 1,
                    column: 46,
                  },
                  end: {
                    line: 1,
                    column: 105,
                  },
                  source:
                    'fn: (tables) => tables.cpu["_value"] + tables.mem["_value"]',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {
                      line: 1,
                      column: 46,
                    },
                    end: {
                      line: 1,
                      column: 48,
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
                      column: 50,
                    },
                    end: {
                      line: 1,
                      column: 105,
                    },
                    source:
                      '(tables) => tables.cpu["_value"] + tables.mem["_value"]',
                  },
                  params: [
                    {
                      type: 'Property',
                      location: {
                        start: {
                          line: 1,
                          column: 51,
                        },
                        end: {
                          line: 1,
                          column: 57,
                        },
                        source: 'tables',
                      },
                      key: {
                        type: 'Identifier',
                        location: {
                          start: {
                            line: 1,
                            column: 51,
                          },
                          end: {
                            line: 1,
                            column: 57,
                          },
                          source: 'tables',
                        },
                        name: 'tables',
                      },
                      value: null,
                    },
                  ],
                  body: {
                    type: 'BinaryExpression',
                    location: {
                      start: {
                        line: 1,
                        column: 62,
                      },
                      end: {
                        line: 1,
                        column: 105,
                      },
                      source: 'tables.cpu["_value"] + tables.mem["_value"]',
                    },
                    operator: '+',
                    left: {
                      type: 'MemberExpression',
                      location: {
                        start: {
                          line: 1,
                          column: 62,
                        },
                        end: {
                          line: 1,
                          column: 83,
                        },
                        source: 'tables.cpu["_value"] ',
                      },
                      object: {
                        type: 'MemberExpression',
                        location: {
                          start: {
                            line: 1,
                            column: 62,
                          },
                          end: {
                            line: 1,
                            column: 83,
                          },
                          source: 'tables.cpu["_value"] ',
                        },
                        object: {
                          type: 'Identifier',
                          location: {
                            start: {
                              line: 1,
                              column: 62,
                            },
                            end: {
                              line: 1,
                              column: 68,
                            },
                            source: 'tables',
                          },
                          name: 'tables',
                        },
                        property: {
                          type: 'Identifier',
                          location: {
                            start: {
                              line: 1,
                              column: 69,
                            },
                            end: {
                              line: 1,
                              column: 72,
                            },
                            source: 'cpu',
                          },
                          name: 'cpu',
                        },
                      },
                      property: {
                        type: 'StringLiteral',
                        location: {
                          start: {
                            line: 1,
                            column: 73,
                          },
                          end: {
                            line: 1,
                            column: 81,
                          },
                          source: '"_value"',
                        },
                        value: '_value',
                      },
                    },
                    right: {
                      type: 'MemberExpression',
                      location: {
                        start: {
                          line: 1,
                          column: 85,
                        },
                        end: {
                          line: 1,
                          column: 105,
                        },
                        source: 'tables.mem["_value"]',
                      },
                      object: {
                        type: 'MemberExpression',
                        location: {
                          start: {
                            line: 1,
                            column: 85,
                          },
                          end: {
                            line: 1,
                            column: 105,
                          },
                          source: 'tables.mem["_value"]',
                        },
                        object: {
                          type: 'Identifier',
                          location: {
                            start: {
                              line: 1,
                              column: 85,
                            },
                            end: {
                              line: 1,
                              column: 91,
                            },
                            source: 'tables',
                          },
                          name: 'tables',
                        },
                        property: {
                          type: 'Identifier',
                          location: {
                            start: {
                              line: 1,
                              column: 92,
                            },
                            end: {
                              line: 1,
                              column: 95,
                            },
                            source: 'mem',
                          },
                          name: 'mem',
                        },
                      },
                      property: {
                        type: 'StringLiteral',
                        location: {
                          start: {
                            line: 1,
                            column: 96,
                          },
                          end: {
                            line: 1,
                            column: 104,
                          },
                          source: '"_value"',
                        },
                        value: '_value',
                      },
                    },
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
