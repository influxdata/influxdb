import _ from 'lodash'
import {
  DashboardTemplate,
  TemplateType,
  CellIncluded,
  LabelIncluded,
  ViewIncluded,
  TaskTemplate,
  TemplateBase,
  Task,
} from 'src/types'
import {IDashboard, Cell} from '@influxdata/influx'
import {client} from 'src/utils/api'

import {
  findIncludedsFromRelationships,
  findLabelIDsToAdd,
  findLabelsToCreate,
  findIncludedFromRelationship,
  findVariablesToCreate,
  findIncludedVariables,
} from 'src/templates/utils/'

// Create Dashboard Templates

export const createDashboardFromTemplate = async (
  template: DashboardTemplate,
  orgID: string
): Promise<IDashboard> => {
  const {content} = template

  if (
    content.data.type !== TemplateType.Dashboard ||
    template.meta.version !== '1'
  ) {
    throw new Error('Can not create dashboard from this template')
  }

  const createdDashboard = await client.dashboards.create({
    ...content.data.attributes,
    orgID,
  })

  if (!createdDashboard || !createdDashboard.id) {
    throw new Error('Failed to create dashboard from template')
  }

  await Promise.all([
    await createDashboardLabelsFromTemplate(template, createdDashboard),
    await createCellsFromTemplate(template, createdDashboard),
  ])
  createVariablesFromTemplate(template, orgID)

  const dashboard = await client.dashboards.get(createdDashboard.id)
  return dashboard
}

const createDashboardLabelsFromTemplate = async (
  template: DashboardTemplate,
  dashboard: IDashboard
) => {
  const templateLabels = await createLabelsFromTemplate(
    template,
    dashboard.orgID
  )
  await client.dashboards.addLabels(dashboard.id, templateLabels)
}

const createLabelsFromTemplate = async <T extends TemplateBase>(
  template: T,
  orgID: string
) => {
  const {
    content: {data, included},
  } = template

  if (!data.relationships || !data.relationships[TemplateType.Label]) {
    return
  }

  const labelRelationships = data.relationships[TemplateType.Label].data

  const labelsIncluded = findIncludedsFromRelationships<LabelIncluded>(
    included,
    labelRelationships
  )

  const existingLabels = await client.labels.getAll()

  const labelsToCreate = findLabelsToCreate(existingLabels, labelsIncluded).map(
    l => ({
      orgID,
      name: _.get(l, 'attributes.name', ''),
      properties: _.get(l, 'attributes.properties', {}),
    })
  )

  const createdLabels = await client.labels.createAll(labelsToCreate)

  // IDs of newly created labels that should be added to dashboard
  const createdLabelIDs = createdLabels.map(l => l.id || '')

  // IDs of existing labels that should be added to dashboard
  const existingLabelIDs = findLabelIDsToAdd(existingLabels, labelsIncluded)

  return [...createdLabelIDs, ...existingLabelIDs]
}

const createCellsFromTemplate = async (
  template: DashboardTemplate,
  createdDashboard: IDashboard
) => {
  const {
    content: {data, included},
  } = template

  if (!data.relationships || !data.relationships[TemplateType.Cell]) {
    return
  }

  const cellRelationships = data.relationships[TemplateType.Cell].data

  const cellsToCreate = findIncludedsFromRelationships<CellIncluded>(
    included,
    cellRelationships
  )

  const pendingCells = cellsToCreate.map(c => {
    const {
      attributes: {x, y, w, h},
    } = c
    return client.dashboards.createCell(createdDashboard.id, {x, y, w, h})
  })

  const cellResponses = await Promise.all(pendingCells)

  createViewsFromTemplate(
    template,
    cellResponses,
    cellsToCreate,
    createdDashboard.id
  )
}

const createViewsFromTemplate = async (
  template: DashboardTemplate,
  cellResponses: Cell[],
  cellsToCreate: CellIncluded[],
  dashboardID: string
) => {
  const viewsToCreate = cellsToCreate.map(c => {
    const {
      content: {included},
    } = template

    const viewRelationship = c.relationships[TemplateType.View].data

    return findIncludedFromRelationship<ViewIncluded>(
      included,
      viewRelationship
    )
  })

  const pendingViews = viewsToCreate.map((v, i) => {
    return client.dashboards.updateView(
      dashboardID,
      cellResponses[i].id,
      v.attributes
    )
  })

  await Promise.all(pendingViews)
}

const createVariablesFromTemplate = async (
  template: DashboardTemplate,
  orgID: string
) => {
  const {
    content: {data, included},
  } = template
  if (!data.relationships || !data.relationships[TemplateType.Variable]) {
    return
  }
  const variablesIncluded = findIncludedVariables(included)

  const existingVariables = await client.variables.getAll()

  const variablesToCreate = findVariablesToCreate(
    existingVariables,
    variablesIncluded
  ).map(v => ({...v.attributes, orgID}))

  await client.variables.createAll(variablesToCreate)
}

export const createTaskFromTemplate = async (
  template: TaskTemplate,
  orgID: string
): Promise<Task> => {
  const {content} = template

  if (
    content.data.type !== TemplateType.Task ||
    template.meta.version !== '1'
  ) {
    throw new Error('Can not create task from this template')
  }

  const flux = content.data.attributes.flux

  const createdTask = await client.tasks.createByOrgID(orgID, flux)

  if (!createdTask || !createdTask.id) {
    throw new Error('Could not create task')
  }

  await createTaskLabelsFromTemplate(template, createdTask)

  const task = await client.tasks.get(createdTask.id)

  return task
}

const createTaskLabelsFromTemplate = async (
  template: TaskTemplate,
  task: Task
) => {
  const templateLabels = await createLabelsFromTemplate(template, task.orgID)
  await client.tasks.addLabels(task.id, templateLabels)
}
