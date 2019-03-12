import {
  labelToRelationship,
  labelToIncluded,
  taskToTemplate,
} from 'src/shared/utils/resourceToTemplate'
import {TemplateType} from '@influxdata/influx'
import {Label, Task, TaskStatus} from 'src/types/v2'

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
