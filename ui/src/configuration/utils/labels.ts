import _ from 'lodash'

import {
  HEX_CODE_CHAR_LENGTH,
  PRESET_LABEL_COLORS,
} from 'src/configuration/constants/LabelColors'

export const randomPresetColor = () =>
  _.sample(PRESET_LABEL_COLORS.slice(1)).colorHex

export const validateLabelName = (labelNames: string[], name: string) => {
  if (name.trim() === '') {
    return 'Label name is required'
  }

  const lowerName = name.toLowerCase()
  const isUnique = !labelNames.find(l => l.toLowerCase() === lowerName)

  if (!isUnique) {
    return 'Label name must be unique'
  }

  return null
}

export const validateHexCode = (colorHex: string): string | null => {
  const isValidLength = colorHex.length === HEX_CODE_CHAR_LENGTH
  const containsValidCharacters =
    colorHex.replace(/[ABCDEF0abcdef123456789]+/g, '') === '#'

  const errorMessage = []

  if (!containsValidCharacters) {
    errorMessage.push('Hexcodes must begin with # and include A-F 0-9')
  }

  if (!isValidLength) {
    errorMessage.push(`Hexcodes must be ${HEX_CODE_CHAR_LENGTH} characters`)
  }

  if (!errorMessage.length) {
    return null
  }

  return errorMessage.join(', ')
}
