import {Label as APILabel} from 'src/api'

/**
 * Required key/value properties for labels
 */
export interface LabelProperties {
  color: string // Every label should have a color
  description?: string
}

export type Label = APILabel & {properties: LabelProperties}
