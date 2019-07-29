import {DashboardColor} from 'src/client'

export type Color = DashboardColor

export interface ColorLabel {
  hex: string
  name: string
}

export enum LabelColorType {
  Preset = 'preset',
  Custom = 'custom',
}

export interface LabelColor {
  id: string
  colorHex: string
  name: string
  type: LabelColorType
}
