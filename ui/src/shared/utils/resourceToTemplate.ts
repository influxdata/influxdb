import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'
import {Task, Label} from 'src/types/v2'
import {ITemplate, TemplateType} from '@influxdata/influx'

const CURRENT_TEMPLATE_VERSION = '1'

const blankTemplate = {
  meta: {version: '0.1.0'},
  content: {data: {}, included: []},
  labels: [],
}

const blankTaskTemplate = {
  ...blankTemplate,
  meta: {
    ...blankTemplate.meta,
  },
  content: {
    ...blankTemplate.content,
    data: {...blankTemplate.content.data, type: TemplateType.Task},
  },
  labels: [
    ...blankTemplate.labels,
    {
      id: '1',
      name: 'influx.task',
      properties: {
        color: 'ffb3b3',
        description: 'This is a template for a task resource on influx 2.0',
      },
    },
  ],
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
): ITemplate => {
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
    ...baseTemplate,
    meta: {
      ...baseTemplate.meta,
      name: templateName,
      version: CURRENT_TEMPLATE_VERSION,
      description: `template created from task: ${taskName}`,
    },
    content: {
      ...baseTemplate.content,
      data: {
        ...baseTemplate.content.data,
        type: TemplateType.Task,
        attributes: taskAttributes,
        relationships: {
          [TemplateType.Label]: {data: relationshipsLabels},
        },
      },
      included: [...baseTemplate.content.included, ...includedLabels],
    },
  }

  return template
}
