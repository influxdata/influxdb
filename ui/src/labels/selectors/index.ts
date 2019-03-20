import {ILabel} from '@influxdata/influx'
import {INFLUX_LABEL_PREFIX} from 'src/labels/constants'

export const viewableLabels = (labels: ILabel[]) =>
  labels.filter(l => !l.name.startsWith(INFLUX_LABEL_PREFIX))
