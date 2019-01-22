import {labelsAPI} from 'src/utils/api'
import {Label} from 'src/types/v2/labels'

import {DEFAULT_LABEL_COLOR_HEX} from 'src/configuration/constants/LabelColors'

// Types
import {Label as APILabel} from 'src/api'

const addLabelDefaults = (l: APILabel): Label => ({
  ...l,
  properties: {
    ...l.properties,
    // add defualt color hex if missing
    color: l.properties.color || DEFAULT_LABEL_COLOR_HEX,
  },
})

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
