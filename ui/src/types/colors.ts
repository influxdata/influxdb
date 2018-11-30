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

export interface ColorConfig {
  color: Color
  label?: string
  isDeletable?: boolean
  disableColor?: boolean
}
