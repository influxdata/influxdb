import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'

import {defaultBuilderConfig} from 'src/shared/utils/view'
import {viewableLabels} from 'src/labels/selectors'

import {Task, Label, Dashboard, Cell, View} from 'src/types'
import {
  TemplateType,
  DocumentCreate,
  ITemplate,
  IVariable as Variable,
} from '@influxdata/influx'
import {DashboardQuery} from 'src/types/dashboards'

const CURRENT_TEMPLATE_VERSION = '1'

const blankTemplate = () => ({
  meta: {version: CURRENT_TEMPLATE_VERSION},
  content: {data: {}, included: []},
  labels: [],
})

const blankTaskTemplate = () => {
  const baseTemplate = blankTemplate()
  return {
    ...baseTemplate,
    meta: {...baseTemplate.meta, type: TemplateType.Task},
    content: {
      ...baseTemplate.content,
      data: {...baseTemplate.content.data, type: TemplateType.Task},
    },
  }
}

const blankVariableTemplate = () => {
  const baseTemplate = blankTemplate()
  return {
    ...baseTemplate,
    meta: {...baseTemplate.meta, type: TemplateType.Variable},
    content: {
      ...baseTemplate.content,
      data: {...baseTemplate.content.data, type: TemplateType.Variable},
    },
  }
}

const blankDashboardTemplate = () => {
  const baseTemplate = blankTemplate()
  return {
    ...baseTemplate,
    meta: {...baseTemplate.meta, type: TemplateType.Dashboard},
    content: {
      ...baseTemplate.content,
      data: {...baseTemplate.content.data, type: TemplateType.Dashboard},
    },
  }
}

export const labelToRelationship = (l: Label) => {
  return {type: TemplateType.Label, id: l.id}
}

export const labelToIncluded = (l: Label) => {
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
  baseTemplate = blankTaskTemplate()
): DocumentCreate => {
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

  const labels = viewableLabels(task.labels)
  const includedLabels = labels.map(l => labelToIncluded(l))
  const relationshipsLabels = labels.map(l => labelToRelationship(l))

  const template = {
    ...baseTemplate,
    meta: {
      ...baseTemplate.meta,
      name: templateName,
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

const viewToIncluded = (view: View) => {
  let properties = view.properties

  if ('queries' in properties) {
    const sanitizedQueries = properties.queries.map((q: DashboardQuery) => {
      return {
        ...q,
        editMode: 'advanced' as 'advanced',
        builderConfig: defaultBuilderConfig(),
      }
    })

    properties = {
      ...properties,
      queries: sanitizedQueries,
    }
  }

  return {
    type: TemplateType.View,
    id: view.id,
    attributes: {name: view.name, properties},
  }
}

const viewToRelationship = (view: View) => ({
  type: TemplateType.View,
  id: view.id,
})

const cellToIncluded = (cell: Cell, views: View[]) => {
  const cellView = views.find(v => v.id === cell.id)
  const viewRelationship = viewToRelationship(cellView)

  const cellAttributes = _.pick(cell, ['x', 'y', 'w', 'h'])

  return {
    id: cell.id,
    type: TemplateType.Cell,
    attributes: cellAttributes,
    relationships: {
      [TemplateType.View]: {
        data: viewRelationship,
      },
    },
  }
}

const cellToRelationship = (cell: Cell) => ({
  type: TemplateType.Cell,
  id: cell.id,
})

export const variableToTemplate = (
  v: Variable,
  dependencies: Variable[],
  baseTemplate = blankVariableTemplate()
) => {
  const variableName = _.get(v, 'name', '')
  const templateName = `${variableName}-Template`
  const variableData = variableToIncluded(v)
  const variableRelationships = dependencies.map(d => variableToRelationship(d))
  const includedDependencies = dependencies.map(d => variableToIncluded(d))
  const includedLabels = v.labels.map(l => labelToIncluded(l))
  const labelRelationships = v.labels.map(l => labelToRelationship(l))

  const includedDependentLabels = _.flatMap(dependencies, d =>
    d.labels.map(l => labelToIncluded(l))
  )

  return {
    ...baseTemplate,
    meta: {
      ...baseTemplate.meta,
      name: templateName,
      description: `template created from variable: ${variableName}`,
    },
    content: {
      ...baseTemplate.content,
      data: {
        ...baseTemplate.content.data,
        ...variableData,
        relationships: {
          [TemplateType.Variable]: {
            data: [...variableRelationships],
          },
          [TemplateType.Label]: {
            data: [...labelRelationships],
          },
        },
      },
      included: [
        ...includedDependencies,
        ...includedLabels,
        ...includedDependentLabels,
      ],
    },
  }
}

const variableToIncluded = (v: Variable) => {
  const variableAttributes = _.pick(v, ['name', 'arguments', 'selected'])
  const labelRelationships = v.labels.map(l => labelToRelationship(l))

  return {
    id: v.id,
    type: TemplateType.Variable,
    attributes: variableAttributes,
    relationships: {
      [TemplateType.Label]: {
        data: [...labelRelationships],
      },
    },
  }
}

const variableToRelationship = (v: Variable) => ({
  type: TemplateType.Variable,
  id: v.id,
})

export const dashboardToTemplate = (
  dashboard: Dashboard,
  views: View[],
  variables: Variable[],
  baseTemplate = blankDashboardTemplate()
): DocumentCreate => {
  const dashboardName = _.get(dashboard, 'name', '')
  const templateName = `${dashboardName}-Template`

  const dashboardAttributes = _.pick(dashboard, ['name', 'description'])

  const dashboardLabels = getDeep<Label[]>(dashboard, 'labels', [])
  const dashboardIncludedLabels = dashboardLabels.map(l => labelToIncluded(l))
  const relationshipsLabels = dashboardLabels.map(l => labelToRelationship(l))

  const cells = getDeep<Cell[]>(dashboard, 'cells', [])
  const includedCells = cells.map(c => cellToIncluded(c, views))
  const relationshipsCells = cells.map(c => cellToRelationship(c))

  const includedVariables = variables.map(v => variableToIncluded(v))
  const variableIncludedLabels = _.flatMap(variables, v =>
    v.labels.map(l => labelToIncluded(l))
  )
  const relationshipsVariables = variables.map(v => variableToRelationship(v))

  const includedViews = views.map(v => viewToIncluded(v))
  const includedLabels = _.uniqBy(
    [...dashboardIncludedLabels, ...variableIncludedLabels],
    'id'
  )

  const template = {
    ...baseTemplate,
    meta: {
      ...baseTemplate.meta,
      name: templateName,
      description: `template created from dashboard: ${dashboardName}`,
    },
    content: {
      ...baseTemplate.content,
      data: {
        ...baseTemplate.content.data,
        type: TemplateType.Dashboard,
        attributes: dashboardAttributes,
        relationships: {
          [TemplateType.Label]: {data: relationshipsLabels},
          [TemplateType.Cell]: {data: relationshipsCells},
          [TemplateType.Variable]: {data: relationshipsVariables},
        },
      },
      included: [
        ...baseTemplate.content.included,
        ...includedLabels,
        ...includedCells,
        ...includedViews,
        ...includedVariables,
      ],
    },
  }

  return template
}

export const templateToExport = (template: ITemplate): DocumentCreate => {
  const pickedTemplate = _.pick(template, ['meta', 'content'])
  const labelsArray = template.labels.map(l => l.name)
  const templateWithLabels = {...pickedTemplate, labels: labelsArray}
  return templateWithLabels
}

export const addOrgIDToTemplate = (
  template: DocumentCreate,
  orgID: string
): DocumentCreate => {
  return {...template, orgID}
}
