import {ILabel} from '@influxdata/influx'
import {TOKEN_LABEL} from 'src/labels/constants'

export const viewableLabels = (labels: ILabel[]) =>
  labels.filter(l => l.name !== TOKEN_LABEL)
