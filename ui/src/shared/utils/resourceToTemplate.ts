import {get, pick, flatMap, uniqBy} from 'lodash'

import {defaultBuilderConfig} from 'src/views/helpers'
import {getLabels} from 'src/resources/selectors'

import {
  AppState,
  Task,
  Label,
  Dashboard,
  DashboardQuery,
  Cell,
  View,
  Variable,
  LabelRelationship,
  LabelIncluded,
} from 'src/types'
import {TemplateType, DocumentCreate, ITemplate} from '@influxdata/influx'

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

export const labelToRelationship = (l: Label): LabelRelationship => {
  return {type: TemplateType.Label, id: l.id}
}

export const labelToIncluded = (l: Label): LabelIncluded => {
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
  state: AppState,
  task: Task,
  baseTemplate = blankTaskTemplate()
): DocumentCreate => {
  const taskName = get(task, 'name', '')
  const templateName = `${taskName}-Template`

  const taskAttributes = pick(task, [
    'status',
    'name',
    'flux',
    'every',
    'cron',
    'offset',
  ])

  const taskLabels = getLabels(state, task.labels)
  const includedLabels = taskLabels.map(label => labelToIncluded(label))
  const relationshipsLabels = taskLabels.map(label =>
    labelToRelationship(label)
  )

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

  const cellAttributes = pick(cell, ['x', 'y', 'w', 'h'])

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
  state: AppState,
  v: Variable,
  dependencies: Variable[],
  baseTemplate = blankVariableTemplate()
) => {
  const labelsByID = state.resources.labels.byID
  const variableName = get(v, 'name', '')
  const templateName = `${variableName}-Template`
  const variableData = variableToIncluded(v, labelsByID)
  const variableRelationships = dependencies.map(d => variableToRelationship(d))
  const includedDependencies = dependencies.map(d =>
    variableToIncluded(d, labelsByID)
  )

  const vLabels = getLabels(state, v.labels)

  const includedLabels = vLabels.map(label => labelToIncluded(label))
  const labelRelationships = vLabels.map(label => labelToRelationship(label))
  const includedDependentLabels = flatMap(dependencies, d => {
    const dLabels = getLabels(state, d.labels)
    return dLabels.map(label => labelToIncluded(label))
  })

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

type LabelsByID = AppState['resources']['labels']['byID']

const variableToIncluded = (v: Variable, labelsByID: LabelsByID) => {
  const variableAttributes = pick(v, ['name', 'arguments', 'selected'])
  const labelRelationships = v.labels
    .map(labelID => {
      const label = labelsByID[labelID]
      if (!label) {
        return null
      }

      return labelToRelationship(label)
    })
    .filter(label => !!label)

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
  state: AppState,
  dashboard: Dashboard,
  cells: Cell[],
  views: View[],
  variables: Variable[],
  baseTemplate = blankDashboardTemplate()
): DocumentCreate => {
  const labelsByID = state.resources.labels.byID
  const dashboardName = get(dashboard, 'name', '')
  const templateName = `${dashboardName}-Template`

  const dashboardAttributes = pick(dashboard, ['name', 'description'])

  const dashboardLabels = getLabels(state, dashboard.labels)
  const dashboardIncludedLabels = dashboardLabels.map(label =>
    labelToIncluded(label)
  )
  const relationshipsLabels = dashboardLabels.map(label =>
    labelToRelationship(label)
  )

  const includedCells = cells.map(c => cellToIncluded(c, views))
  const relationshipsCells = cells.map(c => cellToRelationship(c))

  const includedVariables = variables.map(v =>
    variableToIncluded(v, labelsByID)
  )

  const variableIncludedLabels = flatMap(variables, v =>
    getLabels(state, v.labels).map(label => labelToIncluded(label))
  )

  const relationshipsVariables = variables.map(v => variableToRelationship(v))

  const includedViews = views.map(v => viewToIncluded(v))
  const includedLabels = uniqBy(
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
  const pickedTemplate = pick(template, ['meta', 'content'])
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
