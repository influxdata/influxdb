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
  VariableTemplate,
} from 'src/types'
import {Cell} from '@influxdata/influx'
import * as api from 'src/client'
import {Variable, Dashboard} from 'src/client'
import {
  findIncludedsFromRelationships,
  findLabelsToCreate,
  findIncludedFromRelationship,
  findVariablesToCreate,
  findIncludedVariables,
  hasLabelsRelationships,
  getLabelRelationships,
} from 'src/templates/utils/'

// Create Dashboard Templates

export const createDashboardFromTemplate = async (
  template: DashboardTemplate,
  orgID: string
): Promise<Dashboard> => {
  const {content} = template

  if (
    content.data.type !== TemplateType.Dashboard ||
    template.meta.version !== '1'
  ) {
    throw new Error('Cannot create dashboard from this template')
  }

  const resp = await api.postDashboard({
    data: {
      orgID,
      ...content.data.attributes,
    },
  })

  if (resp.status !== 201) {
    throw new Error('Failed to create dashboard from template')
  }

  // associate imported label id with new label
  const labelMap = await createLabelsFromTemplate(template, orgID)
  await Promise.all([
    await addDashboardLabelsFromTemplate(template, labelMap, resp.data),
    await createCellsFromTemplate(template, resp.data),
  ])

  await createVariablesFromTemplate(template, labelMap, orgID)

  const dashboard = await api.getDashboard({dashboardID: resp.data.id})

  if (dashboard.status !== 200) {
    throw new Error('An error occurred getting the dashboard')
  }
  return dashboard.data
}

const addDashboardLabelsFromTemplate = async (
  template: DashboardTemplate,
  labelMap: LabelMap,
  dashboard: Dashboard
) => {
  try {
    const labelRelationships = getLabelRelationships(template.content.data)
    const [labelID] = labelRelationships.map(l => labelMap[l.id] || '')
    const resp = await api.postDashboardsLabel({
      dashboardID: dashboard.id,
      data: {labelID},
    })
    if (resp.status !== 201) {
      throw new Error('An error occurred when adding the dashboard labels')
    }
  } catch (e) {
    console.error(e)
  }
}

type LabelMap = {[importedID: string]: CreatedLabelID}
type CreatedLabelID = string

const createLabelsFromTemplate = async <T extends TemplateBase>(
  template: T,
  orgID: string
): Promise<LabelMap> => {
  const {
    content: {data, included},
  } = template

  const labeledResources = [data, ...included].filter(r =>
    hasLabelsRelationships(r)
  )

  if (_.isEmpty(labeledResources)) {
    return {}
  }

  const labelRelationships = _.flatMap(labeledResources, r =>
    getLabelRelationships(r)
  )

  const includedLabels = findIncludedsFromRelationships<LabelIncluded>(
    included,
    labelRelationships
  )

  const resp = await api.getLabels({query: {orgID}})

  if (resp.status !== 200) {
    throw new Error('An error occurred getting the labels')
  }

  const labelsToCreate = findLabelsToCreate(
    resp.data.labels,
    includedLabels
  ).map(l => ({
    orgID,
    name: _.get(l, 'attributes.name', ''),
    properties: _.get(l, 'attributes.properties', {}),
  }))

  const promisedLabels = labelsToCreate.map(l => {
    return api
      .postLabel({data: l})
      .then(res => _.get(res, 'res.data.label', ''))
  })

  const createdLabels = await Promise.all(promisedLabels)

  const allLabels = [...createdLabels, ...resp.data.labels]
  const labelMap: LabelMap = {}

  includedLabels.forEach(label => {
    const createdLabel = allLabels.find(l => l.name === label.attributes.name)

    labelMap[label.id] = createdLabel.id
  })

  return labelMap
}

const createCellsFromTemplate = async (
  template: DashboardTemplate,
  createdDashboard: Dashboard
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

  const pendingCells = cellsToCreate.map(async c => {
    const {
      attributes: {x, y, w, h},
    } = c
    return api
      .postDashboardsCell({
        dashboardID: createdDashboard.id,
        data: {x, y, w, h},
      })
      .then(res => res.data)
  })

  const cellResponses = (await Promise.all(pendingCells)) as Cell[]

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
    return api.patchDashboardsCellsView({
      dashboardID,
      cellID: cellResponses[i].id,
      data: v.attributes,
    })
  })

  await Promise.all(pendingViews)
}

const createVariablesFromTemplate = async (
  template: DashboardTemplate | VariableTemplate,
  labelMap: LabelMap,
  orgID: string
) => {
  const {
    content: {data, included},
  } = template
  if (!data.relationships || !data.relationships[TemplateType.Variable]) {
    return
  }
  const variablesIncluded = findIncludedVariables(included)
  const resp = await api.getVariables({query: {orgID}})
  if (resp.status !== 200) {
    throw new Error('An error occurred retrieving the variables')
  }

  const variablesToCreate = findVariablesToCreate(
    resp.data.variables,
    variablesIncluded
  ).map(v => ({...v.attributes, orgID}))

  const pendingVariables = variablesToCreate.map(vars =>
    api.postVariable({data: vars}).then(res => res.data)
  )

  const createdVariables = await Promise.all(pendingVariables)

  const allVars = [...resp.data.variables, ...createdVariables] as Variable[]

  const addLabelsToVars = variablesIncluded.map(async includedVar => {
    const variable = allVars.find(v => v.name === includedVar.attributes.name)
    const labelRelationships = getLabelRelationships(includedVar)
    const [labelID] = labelRelationships.map(l => labelMap[l.id] || '')

    await api.postVariablesLabel({variableID: variable.id, data: {labelID}})
  })

  await Promise.all(addLabelsToVars)
}

export const createTaskFromTemplate = async (
  template: TaskTemplate,
  orgID: string
): Promise<Task> => {
  try {
    const {content} = template

    if (
      content.data.type !== TemplateType.Task ||
      template.meta.version !== '1'
    ) {
      throw new Error('Cannot create task from this template')
    }

    const flux = content.data.attributes.flux

    const postResp = await api.postTask({data: {orgID, flux}})

    if (postResp.status !== 201) {
      throw new Error('Could not create task')
    }

    // associate imported label.id with created label
    const labelMap = await createLabelsFromTemplate(template, orgID)

    await addTaskLabelsFromTemplate(template, labelMap, postResp.data as Task)

    const resp = await api.getTask({taskID: postResp.data.id})

    if (resp.status !== 200) {
      throw new Error(
        'An unexpected error occurred while trying to get the task'
      )
    }

    const task = resp.data as Task

    return task
  } catch (e) {
    console.error(e)
  }
}

const addTaskLabelsFromTemplate = async (
  template: TaskTemplate,
  labelMap: LabelMap,
  task: Task
) => {
  const relationships = getLabelRelationships(template.content.data)
  const [labelID] = relationships.map(l => labelMap[l.id] || '')
  await api.postTasksLabel({taskID: task.id, data: {labelID}})
}

export const createVariableFromTemplate = async (
  template: VariableTemplate,
  orgID: string
) => {
  const {content} = template

  try {
    if (
      content.data.type !== TemplateType.Variable ||
      template.meta.version !== '1'
    ) {
      throw new Error('Cannot create variable from this template')
    }

    const resp = await api.postVariable({
      data: {
        ...content.data.attributes,
        orgID,
      },
    })

    if (resp.status !== 201) {
      throw new Error('Failed to create variable from template')
    }

    // associate imported label.id with created label
    const labelsMap = await createLabelsFromTemplate(template, orgID)

    await createVariablesFromTemplate(template, labelsMap, orgID)

    const variable = await api.getVariable({variableID: resp.data.id})

    if (variable.status !== 200) {
      throw new Error('Failed to get variable from template')
    }

    return variable.data
  } catch (e) {
    console.error(e)
    throw e
  }
}
