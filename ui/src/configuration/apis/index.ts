// API
import {labelsAPI} from 'src/utils/api'

// Utils
import {addLabelDefaults} from 'src/shared/utils/labels'

// Types
import {Label} from 'src/types/v2/labels'

export const getLabels = async (): Promise<Label[]> => {
  const {data} = await labelsAPI.labelsGet()

  return data.labels.map(addLabelDefaults)
}

export const createLabel = async (label: Label): Promise<Label> => {
  const {data} = await labelsAPI.labelsPost(label)

  return addLabelDefaults(data.label)
}

export const deleteLabel = async (label: Label): Promise<void> => {
  await labelsAPI.labelsLabelIDDelete(label.id)
}

export const updateLabel = async (label: Label): Promise<Label> => {
  const {properties} = label

  const {data} = await labelsAPI.labelsLabelIDPatch(label.id, {
    properties,
  })

  return addLabelDefaults(data.label)
}
