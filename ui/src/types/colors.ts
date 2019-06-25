export interface Color {
  // The following values are used in the `type` field:
  //
  // - 'threshold'
  // - 'max'
  // - 'min'
  // - 'text'
  // - 'background'
  // - 'scale'
  //
  // This field drastically changes how a `Color` is used in the UI.
  type: string

  // The `id` field is one of the following:
  //
  // - '0'
  // - '1'
  // - 'base'
  // - A client-generated UUID
  //
  // When the `id` is 'base', the `Color` is treated specially in certain UI
  // features. The `id` is unique within the array of colors for a particular
  // `View`.
  id: string

  // A hex code for this color
  hex: string

  // A name for the hex code for this color; used as a stable identifier
  name: string

  // When a `Color` is being used as a threshold for coloring parts of a
  // visualization, then the `type` field is one of the following:
  //
  // - 'threshold'
  // - 'max'
  // - 'min'
  // - 'text'
  // - 'background'
  //
  // In this case, the `value` is used to determine when the `hex` for this
  // color is applied to parts of a visualization.
  //
  // If the `type` field is 'scale', then this field is unused.
  value: number
}

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
