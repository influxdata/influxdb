import {Label as GenLabel} from 'src/client'

export interface Label extends GenLabel {
  properties?: LabelProperties
}

export interface LabelProperties {
  color: string
  description: string
  [k: string]: string
}
