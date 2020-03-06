import {sample} from 'lodash'
import {Label, RemoteDataState, GenLabel} from 'src/types'

import {HEX_CODE_CHAR_LENGTH, PRESET_LABEL_COLORS} from 'src/labels/constants/'

export const randomPresetColor = () =>
  sample(PRESET_LABEL_COLORS.slice(1)).colorHex

// TODO: Accept a list of label objects instead of strings
// Will have to wait until label types are standardized in the UI
export const validateLabelUniqueness = (
  labelNames: string[],
  name: string
): string | null => {
  if (typeof name !== 'string') {
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
  const beginsWithHash = colorHex.substring(0, 1) === '#'

  const errorMessage = []

  if (!beginsWithHash) {
    errorMessage.push('Hexcodes must begin with #')
  }

  if (!isValidLength) {
    if (errorMessage.length) {
      errorMessage.push(`and must be ${HEX_CODE_CHAR_LENGTH} characters`)
    } else {
      errorMessage.push(`Hexcodes must be ${HEX_CODE_CHAR_LENGTH} characters`)
    }
  }

  if (!errorMessage.length) {
    return null
  }

  return errorMessage.join(', ')
}

const DEFAULT_LABEL_COLOR = '#326BBA'

export const addLabelDefaults = (l: GenLabel): Label => {
  const out = {
    ...l,
    status: RemoteDataState.Done,
    properties: {
      ...l.properties,
      // add default color hex if missing
      color: (l.properties || {}).color || DEFAULT_LABEL_COLOR,
      description: (l.properties || {}).description || '',
    },
  }

  return out
}
