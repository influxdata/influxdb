// Labels can use a different set of brand colors than single stats or gauges
import {Label, RemoteDataState} from 'src/types'
import {LabelColor, LabelColorType} from 'src/types/colors'

export const INFLUX_LABEL_PREFIX = '@influxdata'
export const TOKEN_LABEL = `${INFLUX_LABEL_PREFIX}.token`

export const HEX_CODE_CHAR_LENGTH = 7

export const DEFAULT_LABEL_COLOR_HEX = '#326BBA'

export const EMPTY_LABEL: Label = {
  name: '',
  status: RemoteDataState.Done,
  properties: {
    description: '',
    color: DEFAULT_LABEL_COLOR_HEX,
  },
}

export const CUSTOM_LABEL: LabelColor = {
  id: 'custom',
  colorHex: '#RRRRRR',
  name: 'Custom Hexcode',
  type: LabelColorType.Custom,
}

export const PRESET_LABEL_COLORS: LabelColor[] = [
  CUSTOM_LABEL,
  {
    id: 'label-preset-sapphire',
    colorHex: '#326BBA',
    name: 'Sapphire',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-ocean',
    colorHex: '#4591ED',
    name: 'Ocean',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-pool',
    colorHex: '#22ADF6',
    name: 'Pool',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-laser',
    colorHex: '#00C9FF',
    name: 'Laser',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-hydrogen',
    colorHex: '#6BDFFF',
    name: 'Hydrogen',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-neutrino',
    colorHex: '#BEF0FF',
    name: 'Neutrino',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-void',
    colorHex: '#311F94',
    name: 'Void',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-amethyst',
    colorHex: '#513CC6',
    name: 'Amethyst',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-star',
    colorHex: '#7A65F2',
    name: 'Star',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-comet',
    colorHex: '#9394FF',
    name: 'Comet',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-potassium',
    colorHex: '#B1B6FF',
    name: 'Potassium',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-moonstone',
    colorHex: '#C9D0FF',
    name: 'Moonstone',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-emerald',
    colorHex: '#108174',
    name: 'Emerald',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-viridian',
    colorHex: '#32B08C',
    name: 'Viridian',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-rainforest',
    colorHex: '#4ED8A0',
    name: 'Rainforest',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-honeydew',
    colorHex: '#7CE490',
    name: 'Honeydew',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-krypton',
    colorHex: '#A5F3B4',
    name: 'Krypton',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-wasabi',
    colorHex: '#C6FFD0',
    name: 'Wasabi',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-ruby',
    colorHex: '#BF3D5E',
    name: 'Ruby',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-fire',
    colorHex: '#DC4E58',
    name: 'Fire',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-curacao',
    colorHex: '#F95F53',
    name: 'Curacao',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-dreamsicle',
    colorHex: '#FF8564',
    name: 'Dreamsicle',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-tungsten',
    colorHex: '#FFB6A0',
    name: 'Tungsten',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-marmelade',
    colorHex: '#FFDCCF',
    name: 'Marmelade',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-topaz',
    colorHex: '#E85B1C',
    name: 'Topaz',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-tiger',
    colorHex: '#F48D38',
    name: 'Tiger',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-pineapple',
    colorHex: '#FFB94A',
    name: 'Pineapple',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-thunder',
    colorHex: '#FFD255',
    name: 'Thunder',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-sulfur',
    colorHex: '#FFE480',
    name: 'Sulfur',
    type: LabelColorType.Preset,
  },
  {
    id: 'label-preset-daisy',
    colorHex: '#FFF6B8',
    name: 'Daisy',
    type: LabelColorType.Preset,
  },
]

export const INPUT_ERROR_COLOR = '#0F0E15'
