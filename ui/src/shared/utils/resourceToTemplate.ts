import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'
import {Task, Label} from 'src/types/v2'

export enum TemplateType {
  Label = 'label',
  Task = 'task',
}

const blankTemplate = {meta: {version: '0.1.0'}, data: [], included: []}

const blankTaskTemplate = {
  ...blankTemplate,
  meta: {
    ...blankTemplate.meta,
    labels: [
      {
        id: '1',
        name: 'influx.task',
        properties: {
          color: 'ffb3b3',
          description: 'This is a template for a task resource on influx 2.0',
        },
      },
    ],
  },
  data: [...blankTemplate.data, {type: TemplateType.Task}],
}

const labelToRelationship = (l: Label) => {
  return {type: TemplateType.Label, id: l.id}
}

const labelToIncluded = (l: Label) => {
  return {
    type: TemplateType.Label,
    id: l.id,
    attributes: {
      name: l.name,
      properties: l.properties,
    },
  }
}

export const taskToTemplate = (
  task: Task,
  baseTemplate = blankTaskTemplate
) => {
  const taskName = _.get(task, 'name', '')
  const templateName = `${taskName}-Template`

  const taskAttributes = _.pick(task, [
    'status',
    'name',
    'flux',
    'every',
    'cron',
    'offset',
  ])

  const labels = getDeep<Label[]>(task, 'labels', [])
  const includedLabels = labels.map(l => labelToIncluded(l))
  const relationshipsLabels = labels.map(l => labelToRelationship(l))

  const template = {
    meta: {
      ...baseTemplate.meta,
      name: templateName,
      description: `template created from task: ${taskName}`,
    },
    data: {
      ...baseTemplate.data,
      attributes: taskAttributes,
      relationships: {
        [TemplateType.Label]: {data: relationshipsLabels},
      },
    },
    included: [...baseTemplate.included, ...includedLabels],
  }

  return template
}
