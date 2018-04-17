interface ColorBase {
  type: string
  hex: string
  id: string
  name: string
}

export type ColorString = ColorBase & {value: string}
export type ColorNumber = ColorBase & {value: number}
