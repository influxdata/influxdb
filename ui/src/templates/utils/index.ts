import {TemplateType, LabelIncluded, VariableIncluded} from 'src/types'
import {ILabel, Variable} from '@influxdata/influx'

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
  includeds: {id: string; type: TemplateType}[],
  r: {id: string; type: TemplateType}
): T {
  return includeds.find((i): i is T => i.id === r.id && i.type === r.type) as T
}

export const findLabelIDsToAdd = (
  existingLabels: ILabel[],
  incomingLabels: LabelIncluded[]
): string[] => {
  return existingLabels
    .filter(el => !!incomingLabels.find(l => el.name === l.attributes.name))
    .map(l => l.id || '')
}

export const findLabelsToCreate = (
  currentLabels: ILabel[],
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
