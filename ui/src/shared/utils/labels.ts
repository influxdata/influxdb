// Constants
import {DEFAULT_LABEL_COLOR_HEX} from 'src/configuration/constants/LabelColors'

// Types
import {Label} from 'src/types/v2/labels'
import {Label as APILabel} from 'src/api'

export const addLabelDefaults = (l: APILabel): Label => ({
  ...l,
  properties: {
    ...l.properties,
    // add defualt color hex if missing
    color: l.properties.color || DEFAULT_LABEL_COLOR_HEX,
  },
})
