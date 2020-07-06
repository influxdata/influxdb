import {
  TemplateType,
  LabelIncluded,
  VariableIncluded,
  Relationships,
  LabelRelationship,
  Label,
  Variable,
} from 'src/types'

import {
  Error as PkgError,
  postTemplatesApply,
  TemplateSummary,
} from 'src/client'

export function findIncludedsFromRelationships<
  T extends {id: string; type: TemplateType}
>(
  includeds: {id: string; type: TemplateType}[],
  relationships: {id: string; type: TemplateType}[]
): T[] {
  let intersection = []
  relationships.forEach(r => {
    const included = findIncludedFromRelationship<T>(includeds, r)
    if (included) {
      intersection = [...intersection, included]
    }
  })
  return intersection
}

export function findIncludedFromRelationship<
  T extends {id: string; type: TemplateType}
>(
  included: {id: string; type: TemplateType}[],
  r: {id: string; type: TemplateType}
): T {
  return included.find((i): i is T => i.id === r.id && i.type === r.type)
}

export const findLabelsToCreate = (
  currentLabels: Label[],
  labels: LabelIncluded[]
): LabelIncluded[] => {
  return labels.filter(
    l => !currentLabels.find(el => el.name === l.attributes.name)
  )
}

export const findIncludedVariables = (included: {type: TemplateType}[]) => {
  return included.filter(
    (r): r is VariableIncluded => r.type === TemplateType.Variable
  )
}

export const findVariablesToCreate = (
  existingVariables: Variable[],
  incomingVariables: VariableIncluded[]
): VariableIncluded[] => {
  return incomingVariables.filter(
    v => !existingVariables.find(ev => ev.name === v.attributes.name)
  )
}

export const hasLabelsRelationships = (resource: {
  relationships?: Relationships
}) => !!resource.relationships && !!resource.relationships[TemplateType.Label]

export const getLabelRelationships = (resource: {
  relationships?: Relationships
}): LabelRelationship[] => {
  if (!hasLabelsRelationships(resource)) {
    return []
  }

  return [].concat(resource.relationships[TemplateType.Label].data)
}

export const getIncludedLabels = (included: {type: TemplateType}[]) =>
  included.filter((i): i is LabelIncluded => i.type === TemplateType.Label)

// See https://github.com/influxdata/community-templates/
// an example of a url that works with this function:
// https://github.com/influxdata/community-templates/tree/master/csgo
export const getTemplateNameFromGithubUrl = (url: string): string => {
  if (!url.includes('https://github.com/influxdata/community-templates/')) {
    throw new Error(
      "We're only going to fetch from influxdb's github repo right now"
    )
  }
  const [, name] = url.split('/tree/master/')
  return name
}

export const getGithubUrlFromTemplateName = (templateName: string): string => {
  return `https://github.com/influxdata/community-templates/tree/master/${templateName}`
}

export const getRawUrlFromGithub = repoUrl => {
  return repoUrl
    .replace('github.com', 'raw.githubusercontent.com')
    .replace('tree/', '')
}

const applyTemplates = async params => {
  const resp = await postTemplatesApply(params)
  if (resp.status >= 300) {
    throw new Error((resp.data as PkgError).message)
  }

  const summary = resp.data as TemplateSummary
  return summary
}

export const reviewTemplate = async (orgID: string, templateUrl: string) => {
  const params = {
    data: {
      dryRun: true,
      orgID,
      remotes: [{url: templateUrl}],
    },
  }

  return applyTemplates(params)
}

export const installTemplate = async (orgID: string, templateUrl: string) => {
  const params = {
    data: {
      dryRun: false,
      orgID,
      remotes: [{url: templateUrl}],
    },
  }

  return applyTemplates(params)
}
