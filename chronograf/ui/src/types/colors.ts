interface ColorBase {
  type: string
  hex: string
  id: string
  name: string
}

export type ColorString = ColorBase & {value: string}
export type ColorNumber = ColorBase & {value: number}
export type LineColor = ColorString
export type GaugeColor = ColorString | ColorNumber

export interface ThresholdColor {
  hex: string
  name: string
}
