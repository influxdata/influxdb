export default {
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
