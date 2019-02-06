import {Label as LabelAPI} from '@influxdata/influx'

/**
 * Required key/value properties for labels
 */
export interface LabelProperties {
  color: string // Every label should have a color
  description?: string
}

export type Label = LabelAPI & {properties: LabelProperties}
