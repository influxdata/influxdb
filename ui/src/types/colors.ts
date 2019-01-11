export interface Color {
  type: string
  hex: string
  id: string
  name: string
  value: number
}

export interface ColorLabel {
  hex: string
  name: string
}

export interface ThresholdConfig {
  color: Color
  label?: string
  isDeletable?: boolean
  isBase?: boolean
  disableColor?: boolean
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
