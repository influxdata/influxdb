import _ from 'lodash'

import {
  HEX_CODE_CHAR_LENGTH,
  PRESET_LABEL_COLORS,
} from 'src/configuration/constants/LabelColors'

export const randomPresetColor = () =>
  _.sample(PRESET_LABEL_COLORS.slice(1)).colorHex

// TODO: Accept a list of label objects instead of strings
// Will have to wait until label types are standardized in the UI
export const validateLabelUniqueness = (
  labelNames: string[],
  name: string
): string | null => {
  if (!name) {
    return null
  }

  if (name.trim() === '') {
    return 'Label name is required'
  }

  const isNameUnique = !labelNames.find(
    labelName => labelName.toLowerCase() === name.toLowerCase()
  )

  if (!isNameUnique) {
    return 'There is already a label with that name'
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
