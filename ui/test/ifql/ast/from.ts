export default {
  type: 'Program',
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
          column: 21,
        },
        source: 'from(db: "telegraf")',
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
    },
  ],
}
