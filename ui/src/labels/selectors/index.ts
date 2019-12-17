import {Label} from 'src/types'
import {INFLUX_LABEL_PREFIX} from 'src/labels/constants'

export const viewableLabels = (labels: Label[]) =>
  labels.filter(l => !l.name.startsWith(INFLUX_LABEL_PREFIX))
