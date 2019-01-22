// Labels can use a different set of brand colors than single stats or gauges
import {LabelType} from 'src/clockface'
import {LabelColor, LabelColorType} from 'src/types/colors'

export const HEX_CODE_CHAR_LENGTH = 7

export const DEFAULT_LABEL_COLOR_HEX = '#326BBA'

export const EMPTY_LABEL: LabelType = {
  id: 'newLabel',
  name: '',
  description: '',
  colorHex: DEFAULT_LABEL_COLOR_HEX,
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
]

export const INPUT_ERROR_COLOR = '#0F0E15'
