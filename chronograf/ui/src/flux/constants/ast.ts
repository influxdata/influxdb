export const emptyAST = {
  type: 'Program',
  location: {
    start: {
      line: 1,
      column: 1,
    },
    end: {
      line: 1,
      column: 1,
    },
    source: '',
  },
  body: [],
}

export const ast = {
  type: 'File',
  start: 0,
  end: 22,
  loc: {
    start: {
      line: 1,
      column: 0,
    },
    end: {
      line: 1,
      column: 22,
    },
  },
  program: {
    type: 'Program',
    start: 0,
    end: 22,
    loc: {
      start: {
        line: 1,
        column: 0,
      },
      end: {
        line: 1,
        column: 22,
      },
    },
    sourceType: 'module',
    body: [
      {
        type: 'ExpressionStatement',
        start: 0,
        end: 22,
        loc: {
          start: {
            line: 1,
            column: 0,
          },
          end: {
            line: 1,
            column: 22,
          },
        },
        expression: {
          type: 'CallExpression',
          start: 0,
          end: 22,
          loc: {
            start: {
              line: 1,
              column: 0,
            },
            end: {
              line: 1,
              column: 22,
            },
          },
          callee: {
            type: 'Identifier',
            start: 0,
            end: 4,
            loc: {
              start: {
                line: 1,
                column: 0,
              },
              end: {
                line: 1,
                column: 4,
              },
              identifierName: 'from',
            },
            name: 'from',
          },
          arguments: [
            {
              type: 'ObjectExpression',
              start: 5,
              end: 21,
              loc: {
                start: {
                  line: 1,
                  column: 5,
                },
                end: {
                  line: 1,
                  column: 21,
                },
              },
              properties: [
                {
                  type: 'ObjectProperty',
                  start: 6,
                  end: 20,
                  loc: {
                    start: {
                      line: 1,
                      column: 6,
                    },
                    end: {
                      line: 1,
                      column: 20,
                    },
                  },
                  method: false,
                  shorthand: false,
                  computed: false,
                  key: {
                    type: 'Identifier',
                    start: 6,
                    end: 8,
                    loc: {
                      start: {
                        line: 1,
                        column: 6,
                      },
                      end: {
                        line: 1,
                        column: 8,
                      },
                      identifierName: 'db',
                    },
                    name: 'db',
                  },
                  value: {
                    type: 'StringLiteral',
                    start: 10,
                    end: 20,
                    loc: {
                      start: {
                        line: 1,
                        column: 10,
                      },
                      end: {
                        line: 1,
                        column: 20,
                      },
                    },
                    extra: {
                      rawValue: 'telegraf',
                      raw: 'telegraf',
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
    directives: [],
  },
}
