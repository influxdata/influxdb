export default {
  type: 'Program',
  location: {
    start: {line: 1, column: 1},
    end: {line: 1, column: 61},
    source:
      'bux = "ASDFASDFASDF"\nfoo = from(db: "foo")\t\nfrom(db: bux)\n\n\n',
  },
  body: [
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 1, column: 1},
        end: {line: 1, column: 21},
        source: 'bux = "ASDFASDFASDF"',
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
              source: '"ASDFASDFASDF"',
            },
            value: 'ASDFASDFASDF',
          },
        },
      ],
    },
    {
      type: 'VariableDeclaration',
      location: {
        start: {line: 2, column: 1},
        end: {line: 2, column: 22},
        source: 'foo = from(db: "foo")',
      },
      declarations: [
        {
          type: 'VariableDeclarator',
          id: {
            type: 'Identifier',
            location: {
              start: {line: 2, column: 1},
              end: {line: 2, column: 4},
              source: 'foo',
            },
            name: 'foo',
          },
          init: {
            type: 'CallExpression',
            location: {
              start: {line: 2, column: 7},
              end: {line: 2, column: 22},
              source: 'from(db: "foo")',
            },
            callee: {
              type: 'Identifier',
              location: {
                start: {line: 2, column: 7},
                end: {line: 2, column: 11},
                source: 'from',
              },
              name: 'from',
            },
            arguments: [
              {
                type: 'ObjectExpression',
                location: {
                  start: {line: 2, column: 12},
                  end: {line: 2, column: 21},
                  source: 'db: "foo"',
                },
                properties: [
                  {
                    type: 'Property',
                    location: {
                      start: {line: 2, column: 12},
                      end: {line: 2, column: 21},
                      source: 'db: "foo"',
                    },
                    key: {
                      type: 'Identifier',
                      location: {
                        start: {line: 2, column: 12},
                        end: {line: 2, column: 14},
                        source: 'db',
                      },
                      name: 'db',
                    },
                    value: {
                      type: 'StringLiteral',
                      location: {
                        start: {line: 2, column: 16},
                        end: {line: 2, column: 21},
                        source: '"foo"',
                      },
                      value: 'foo',
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
        start: {line: 3, column: 1},
        end: {line: 3, column: 14},
        source: 'from(db: bux)',
      },
      expression: {
        type: 'CallExpression',
        location: {
          start: {line: 3, column: 1},
          end: {line: 3, column: 14},
          source: 'from(db: bux)',
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
              end: {line: 3, column: 13},
              source: 'db: bux',
            },
            properties: [
              {
                type: 'Property',
                location: {
                  start: {line: 3, column: 6},
                  end: {line: 3, column: 13},
                  source: 'db: bux',
                },
                key: {
                  type: 'Identifier',
                  location: {
                    start: {line: 3, column: 6},
                    end: {line: 3, column: 8},
                    source: 'db',
                  },
                  name: 'db',
                },
                value: {
                  type: 'Identifier',
                  location: {
                    start: {line: 3, column: 10},
                    end: {line: 3, column: 13},
                    source: 'bux',
                  },
                  name: 'bux',
                },
              },
            ],
          },
        ],
      },
    },
  ],
}
