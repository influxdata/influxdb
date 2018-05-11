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
