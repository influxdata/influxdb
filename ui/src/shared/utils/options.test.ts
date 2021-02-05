import {
  getProperty,
  setProperty,
  deleteProperty,
  optionFromFile,
  optionFromPackage,
  setOption,
  deleteOption,
} from 'src/shared/utils/options'

describe('get object properties', () => {
  it('exists', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    expect(getProperty(obj, 'a')).toEqual({
      type: 'IntegerLiteral' as const,
      value: 5,
    })
  })
  it('non-existing', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    expect(getProperty(obj, 'b')).toBeUndefined()
  })
})
describe('set object properties', () => {
  it('exists', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'b',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 6,
          },
        },
      ],
    }
    setProperty(obj, 'b', {
      type: 'IntegerLiteral' as const,
      value: 7,
    })
    expect(obj).toEqual({
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'b',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 7,
          },
        },
      ],
    })
  })
  it('non-existing', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    setProperty(obj, 'b', {
      type: 'IntegerLiteral' as const,
      value: 6,
    })
    expect(obj).toEqual({
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'b',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 6,
          },
        },
      ],
    })
  })
})
describe('delete object properties', () => {
  it('exists', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    deleteProperty(obj, 'a')
    expect(obj).toEqual({
      type: 'ObjectExpression' as const,
      properties: [],
    })
  })
  it('non-existing', () => {
    const obj = {
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    deleteProperty(obj, 'b')
    expect(obj).toEqual({
      type: 'ObjectExpression' as const,
      properties: [
        {
          type: 'Property' as const,
          key: {
            type: 'Identifier' as const,
            name: 'a',
          },
          value: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    })
  })
})
describe('retrieve options from file', () => {
  it('exists', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'b',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 6,
            },
          },
        },
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'c',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 7,
          },
        },
      ],
    }
    expect(optionFromFile(file, 'b')).toEqual({
      type: 'IntegerLiteral' as const,
      value: 6,
    })
  })
  it('non-existing', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    expect(optionFromFile(file, 'c')).toBeUndefined()
  })
})
describe('retrieve options from package', () => {
  it('exists', () => {
    const pkg = {
      type: 'Package' as const,
      package: 'pkg',
      files: [
        {
          type: 'File' as const,
          package: {
            type: 'PackageClause' as const,
            name: {
              type: 'Identifier' as const,
              name: 'pkg',
            },
          },
          imports: [],
          body: [
            {
              type: 'OptionStatement' as const,
              assignment: {
                type: 'VariableAssignment' as const,
                id: {
                  type: 'Identifier' as const,
                  name: 'a',
                },
                init: {
                  type: 'IntegerLiteral' as const,
                  value: 5,
                },
              },
            },
          ],
        },
        {
          type: 'File' as const,
          package: {
            type: 'PackageClause' as const,
            name: {
              type: 'Identifier' as const,
              name: 'pkg',
            },
          },
          imports: [],
          body: [
            {
              type: 'OptionStatement' as const,
              assignment: {
                type: 'VariableAssignment' as const,
                id: {
                  type: 'Identifier' as const,
                  name: 'b',
                },
                init: {
                  type: 'IntegerLiteral' as const,
                  value: 6,
                },
              },
            },
          ],
        },
      ],
    }
    expect(optionFromPackage(pkg, 'b')).toEqual({
      type: 'IntegerLiteral' as const,
      value: 6,
    })
  })
  it('non-existing', () => {
    const pkg = {
      type: 'Package' as const,
      package: 'pkg',
      files: [],
    }
    expect(optionFromPackage(pkg, 'a')).toBeUndefined()
  })
})
describe('delete options', () => {
  it('exists', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'a',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 5,
            },
          },
        },
      ],
    }
    deleteOption(file, 'a')
    expect(file).toEqual({
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [],
    })
  })
  it('non-existing', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    }
    deleteOption(file, 'a')
    expect(file).toEqual({
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
      ],
    })
  })
})
describe('set options', () => {
  it('exists', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'b',
            },
            init: {
              type: 'StringLiteral' as const,
              value: 'Ryan ❤️ Yankees',
            },
          },
        },
      ],
    }
    setOption(file, 'b', {
      type: 'IntegerLiteral' as const,
      value: 7,
    })
    expect(file).toEqual({
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'VariableAssignment' as const,
          id: {
            type: 'Identifier' as const,
            name: 'a',
          },
          init: {
            type: 'IntegerLiteral' as const,
            value: 5,
          },
        },
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'b',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 7,
            },
          },
        },
      ],
    })
  })
  it('non-existing', () => {
    const file = {
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'a',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 5,
            },
          },
        },
      ],
    }
    setOption(file, 'b', {
      type: 'IntegerLiteral' as const,
      value: 6,
    })
    expect(file).toEqual({
      type: 'File' as const,
      package: {
        type: 'PackageClause' as const,
        name: {
          type: 'Identifier' as const,
          name: 'pkg',
        },
      },
      imports: [],
      body: [
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'b',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 6,
            },
          },
        },
        {
          type: 'OptionStatement' as const,
          assignment: {
            type: 'VariableAssignment' as const,
            id: {
              type: 'Identifier' as const,
              name: 'a',
            },
            init: {
              type: 'IntegerLiteral' as const,
              value: 5,
            },
          },
        },
      ],
    })
  })
})
