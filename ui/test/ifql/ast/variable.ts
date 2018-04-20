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
