import {
  labelToRelationship,
  labelToIncluded,
  taskToTemplate,
  variableToTemplate,
} from 'src/shared/utils/resourceToTemplate'
import {TemplateType, Variable} from '@influxdata/influx'
import {Label, Task, TaskStatus} from 'src/types'

const myfavelabel: Label = {
  id: '1',
  name: '1label',
  properties: {color: 'fffff', description: 'omg'},
}

const myfavetask: Task = {
  authorizationID: '037b084ed9abc000',
  every: '24h0m0s',
  flux:
    'option task = {name: "lala", every: 86400000000000ns, offset: 60000000000ns}\n\nfrom(bucket: "defnuck")\n\t|> range(start: -task.every)',
  id: '037b0877b359a000',
  labels: [
    {
      id: '037b0c86a92a2000',
      name: 'yum',
      properties: {color: '#FF8564', description: ''},
    },
  ],
  name: 'lala',
  offset: '1m0s',
  org: 'org',
  orgID: '037b084ec8ebc000',
  status: TaskStatus.Active,
}

const myVariable: Variable = {
  id: '039ae3b3b74b0000',
  orgID: '039aa15b38cb0000',
  name: 'beep',
  selected: null,
  arguments: {
    type: 'query',
    values: {
      query: 'test!',
      language: 'flux',
    },
  },
}

describe('resourceToTemplate', () => {
  describe('labelToRelationship', () => {
    it('converts a label to a relationship struct', () => {
      const actual = labelToRelationship(myfavelabel)
      const expected = {type: TemplateType.Label, id: myfavelabel.id}

      expect(actual).toEqual(expected)
    })
  })
  describe('labelToIncluded', () => {
    it('converts a label to a data structure in included', () => {
      const actual = labelToIncluded(myfavelabel)
      const expected = {
        type: TemplateType.Label,
        id: myfavelabel.id,
        attributes: {
          name: myfavelabel.name,
          properties: {
            color: myfavelabel.properties.color,
            description: myfavelabel.properties.description,
          },
        },
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('variableToTemplate', () => {
    it('converts a variable to a template', () => {
      const actual = variableToTemplate(myVariable, [])
      const expected = {
        meta: {
          version: '1',
          name: 'beep-Template',
          description: 'template created from variable: beep',
        },
        content: {
          data: {
            type: 'variable',
            id: '039ae3b3b74b0000',
            attributes: {
              name: 'beep',
              arguments: {
                type: 'query',
                values: {
                  query: 'test!',
                  language: 'flux',
                },
              },
              selected: null,
            },
            relationships: {
              variable: {
                data: [],
              },
            },
          },
          included: [],
        },
        labels: [],
      }

      expect(actual).toEqual(expected)
    })

    it('converts a variable with dependencies to a template', () => {
      const parentArgs = {
        values: {...myVariable.arguments.values, query: `v.${myVariable.name}`},
      }
      const parentVar = {
        ...myVariable,
        id: '123Parent',
        name: 'Parent Var',
        arguments: {
          ...myVariable.arguments,
          ...parentArgs,
        },
      }
      const actual = variableToTemplate(parentVar, [myVariable])
      const expected = {
        meta: {
          version: '1',
          name: 'Parent Var-Template',
          description: 'template created from variable: Parent Var',
        },
        content: {
          data: {
            type: 'variable',
            id: '123Parent',
            attributes: {
              name: 'Parent Var',
              arguments: {
                type: 'query',
                values: {
                  query: 'v.beep',
                  language: 'flux',
                },
              },
              selected: null,
            },
            relationships: {
              variable: {
                data: [
                  {
                    type: 'variable',
                    id: '039ae3b3b74b0000',
                  },
                ],
              },
            },
          },
          included: [
            {
              type: 'variable',
              id: '039ae3b3b74b0000',
              attributes: {
                name: 'beep',
                arguments: {
                  type: 'query',
                  values: {
                    query: 'test!',
                    language: 'flux',
                  },
                },
                selected: null,
              },
            },
          ],
        },
        labels: [],
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('taskToTemplate', () => {
    it('converts a task to a template', () => {
      const actual = taskToTemplate(myfavetask)
      const expected = {
        content: {
          data: {
            type: 'task',
            attributes: {
              every: '24h0m0s',
              flux:
                'option task = {name: "lala", every: 86400000000000ns, offset: 60000000000ns}\n\nfrom(bucket: "defnuck")\n\t|> range(start: -task.every)',
              name: 'lala',
              offset: '1m0s',
              status: 'active',
            },
            relationships: {
              label: {
                data: [
                  {
                    id: '037b0c86a92a2000',
                    type: 'label',
                  },
                ],
              },
            },
          },
          included: [
            {
              attributes: {
                name: 'yum',
                properties: {
                  color: '#FF8564',
                  description: '',
                },
              },
              id: '037b0c86a92a2000',
              type: TemplateType.Label,
            },
          ],
        },
        labels: [],
        meta: {
          description: 'template created from task: lala',
          name: 'lala-Template',
          version: '1',
        },
      }

      expect(actual).toEqual(expected)
    })
  })
})
