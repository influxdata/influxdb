// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType, GenLabel} from 'src/types'

// Utils
import {addLabelDefaults} from 'src/labels/utils'

/* Labels */
export const labelSchema = new schema.Entity(
  ResourceType.Labels,
  {},
  {
    processStrategy: (label: GenLabel) => addLabelDefaults(label),
  }
)

export const arrayOfLabels = [labelSchema]
