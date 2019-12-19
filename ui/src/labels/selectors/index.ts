// Types
import {Label, AppState} from 'src/types'

// Constants
import {INFLUX_LABEL_PREFIX} from 'src/labels/constants'

export const getLabelsByID = ({resources}: AppState, labelIDs: string[]) => {
  return labelIDs.map(id => resources.labels.byID[id])
}

export const getAllLabels = ({resources}: AppState) => {
  return resources.labels.allIDs.map(id => resources.labels.byID[id])
}

export const viewableLabels = (labels: Label[]) =>
  labels.filter(l => !l.name.startsWith(INFLUX_LABEL_PREFIX))
