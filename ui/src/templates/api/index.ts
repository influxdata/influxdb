// Libraries
import {get, isEmpty, flatMap} from 'lodash'
import {normalize} from 'normalizr'

// Schemas
import {arrayOfVariables, variableSchema} from 'src/schemas/variables'
import {taskSchema} from 'src/schemas/tasks'

// Utils
import {
  findIncludedsFromRelationships,
  findLabelsToCreate,
  findIncludedFromRelationship,
  findVariablesToCreate,
  findIncludedVariables,
  hasLabelsRelationships,
  getLabelRelationships,
} from 'src/templates/utils/'
import {addLabelDefaults} from 'src/labels/utils'

// API
import {
  getDashboard as apiGetDashboard,
  getTask as apiGetTask,
  postTask as apiPostTask,
  postTasksLabel as apiPostTasksLabel,
  getLabels as apiGetLabels,
  postLabel as apiPostLabel,
  getVariable as apiGetVariable,
  getVariables as apiGetVariables,
  postVariable as apiPostVariable,
  postVariablesLabel as apiPostVariablesLabel,
  postDashboard as apiPostDashboard,
  postDashboardsLabel as apiPostDashboardsLabel,
  postDashboardsCell as apiPostDashboardsCell,
  patchDashboardsCellsView as apiPatchDashboardsCellsView,
} from 'src/client'
import {addDashboardDefaults} from 'src/schemas/dashboards'

// Types
import {
  TaskEntities,
  DashboardTemplate,
  Dashboard,
  TemplateType,
  Cell,
  CellIncluded,
  LabelIncluded,
  ViewIncluded,
  TaskTemplate,
  TemplateBase,
  Task,
  VariableTemplate,
  Variable,
  VariableEntities,
  PostVariable,
} from 'src/types'

// Create Dashboard Templates
export const createDashboardFromTemplate = async (
  template: DashboardTemplate,
  orgID: string
): Promise<Dashboard> => {
  try {
    const {content} = template

    if (
      content.data.type !== TemplateType.Dashboard ||
      template.meta.version !== '1'
    ) {
      throw new Error('Cannot create dashboard from this template')
    }

    const resp = await apiPostDashboard({
      data: {
        orgID,
        ...content.data.attributes,
      },
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const createdDashboard = addDashboardDefaults(resp.data as Dashboard)

    // associate imported label id with new label
    const labelMap = await createLabelsFromTemplate(template, orgID)

    await Promise.all([
      await addDashboardLabelsFromTemplate(
        template,
        labelMap,
        createdDashboard
      ),
      await createCellsFromTemplate(template, createdDashboard),
    ])

    await createVariablesFromTemplate(template, labelMap, orgID)

    const getResp = await apiGetDashboard({dashboardID: resp.data.id})

    if (getResp.status !== 200) {
      throw new Error(getResp.data.message)
    }

    return getResp.data as Dashboard
  } catch (error) {
    console.error(error)
    throw new Error(error.message)
  }
}

const addDashboardLabelsFromTemplate = async (
  template: DashboardTemplate,
  labelMap: LabelMap,
  dashboard: Dashboard
) => {
  try {
    const labelRelationships = getLabelRelationships(template.content.data)
    const labelIDs = labelRelationships.map(l => labelMap[l.id] || '')
    const pending = labelIDs.map(labelID =>
      apiPostDashboardsLabel({dashboardID: dashboard.id, data: {labelID}})
    )
    const resolved = await Promise.all(pending)
    if (resolved.length > 0 && resolved.some(r => r.status !== 201)) {
      throw new Error(
        'An error occurred adding dashboard labels from the template'
      )
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

  if (isEmpty(labeledResources)) {
    return {}
  }

  const labelRelationships = flatMap(labeledResources, r =>
    getLabelRelationships(r)
  )

  const includedLabels = findIncludedsFromRelationships<LabelIncluded>(
    included,
    labelRelationships
  )

  const resp = await apiGetLabels({query: {orgID}})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const existingLabels = resp.data.labels.map(l => addLabelDefaults(l))

  const foundLabelsToCreate = findLabelsToCreate(
    existingLabels,
    includedLabels
  ).map(l => ({
    orgID,
    name: get(l, 'attributes.name', ''),
    properties: get(l, 'attributes.properties', {}),
  }))

  const promisedLabels = foundLabelsToCreate.map(lab => {
    return apiPostLabel({
      data: lab,
    })
      .then(res => {
        const out = get(res, 'data.label', '')
        return out
      })
      .then(lab => addLabelDefaults(lab))
  })

  const createdLabels = await Promise.all(promisedLabels)

  const allLabels = [...createdLabels, ...existingLabels]

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

  const pendingCells = cellsToCreate.map(c => {
    const {
      attributes: {x, y, w, h},
    } = c
    return apiPostDashboardsCell({
      dashboardID: createdDashboard.id,
      data: {x, y, w, h},
    })
  })

  const cellResponses = await Promise.all(pendingCells)

  if (cellResponses.length > 0 && cellResponses.some(r => r.status !== 201)) {
    throw new Error('An error occurred creating cells from the templates')
  }

  const responses = cellResponses.map(resp => resp.data as Cell)

  createViewsFromTemplate(
    template,
    responses,
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
    return apiPatchDashboardsCellsView({
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

  const resp = await apiGetVariables({query: {orgID}})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const normVariables = normalize<Variable, VariableEntities, string[]>(
    resp.data.variables,
    arrayOfVariables
  )

  const variables = Object.values<Variable>(
    get(normVariables, 'entities.variables', {})
  )

  const variablesToCreate = findVariablesToCreate(
    variables,
    variablesIncluded
  ).map(v => ({...v.attributes, orgID}))

  const pendingVariables = variablesToCreate.map(vars =>
    apiPostVariable({data: vars as PostVariable})
  )

  const resolvedVariables = await Promise.all(pendingVariables)

  if (
    resolvedVariables.length > 0 &&
    resolvedVariables.every(r => r.status !== 201)
  ) {
    throw new Error('An error occurred creating the variables from templates')
  }

  const createdVariables = await Promise.all(pendingVariables)

  const normCreated = createdVariables.map(v => {
    const normVar = normalize<Variable, VariableEntities, string>(
      v.data,
      variableSchema
    )
    return normVar.entities.variables[normVar.result]
  })

  const allVars = [...variables, ...normCreated]

  const addLabelsToVars = variablesIncluded.map(async includedVar => {
    const variable = allVars.find(v => v.name === includedVar.attributes.name)
    const labelRelationships = getLabelRelationships(includedVar)
    const labelIDs = labelRelationships.map(l => labelMap[l.id] || '')
    const pending = labelIDs.map(async labelID => {
      await apiPostVariablesLabel({variableID: variable.id, data: {labelID}})
    })
    await Promise.all(pending)
  })

  await Promise.all(addLabelsToVars)
}

export const createTaskFromTemplate = async (
  template: TaskTemplate,
  orgID: string
): Promise<Task> => {
  const {content} = template
  try {
    if (
      content.data.type !== TemplateType.Task ||
      template.meta.version !== '1'
    ) {
      throw new Error('Cannot create task from this template')
    }

    const flux = content.data.attributes.flux

    const postResp = await apiPostTask({data: {orgID, flux}})

    if (postResp.status !== 201) {
      throw new Error(postResp.data.message)
    }

    const {entities, result} = normalize<Task, TaskEntities, string>(
      postResp.data,
      taskSchema
    )

    const postedTask = entities.tasks[result]

    // associate imported label.id with created label
    const labelMap = await createLabelsFromTemplate(template, orgID)

    await addTaskLabelsFromTemplate(template, labelMap, postedTask)

    const resp = await apiGetTask({taskID: postedTask.id})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    return postedTask
  } catch (e) {
    console.error(e)
  }
}

const addTaskLabelsFromTemplate = async (
  template: TaskTemplate,
  labelMap: LabelMap,
  task: Task
) => {
  try {
    const relationships = getLabelRelationships(template.content.data)
    const labelIDs = relationships.map(l => labelMap[l.id] || '')
    const pending = labelIDs.map(labelID =>
      apiPostTasksLabel({taskID: task.id, data: {labelID}})
    )
    const resolved = await Promise.all(pending)
    if (resolved.length > 0 && resolved.some(r => r.status !== 201)) {
      throw new Error('An error occurred adding task labels from the templates')
    }
  } catch (error) {
    console.error(error)
  }
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

    const resp = await apiPostVariable({
      data: {
        ...content.data.attributes,
        orgID,
      },
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    // associate imported label.id with created label
    const labelsMap = await createLabelsFromTemplate(template, orgID)

    await createVariablesFromTemplate(template, labelsMap, orgID)

    const variable = await apiGetVariable({variableID: resp.data.id})

    if (variable.status !== 200) {
      throw new Error(variable.data.message)
    }

    return variable.data
  } catch (error) {
    console.error(error)
  }
}
