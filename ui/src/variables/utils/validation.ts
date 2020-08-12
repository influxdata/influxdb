import {Variable} from 'src/types'
import {
  TIME_RANGE_START,
  TIME_RANGE_STOP,
  WINDOW_PERIOD,
} from 'src/variables/constants'

const reservedVarNames = [TIME_RANGE_START, TIME_RANGE_STOP, WINDOW_PERIOD]

export const validateVariableName = (
  varName: string,
  variables: Variable[]
): {error: string | null} => {
  if ((varName || '').match(/^\s*$/)) {
    return {error: 'Variable name cannot be empty'}
  }

  const lowerName = varName.toLocaleLowerCase()

  const reservedMatch = reservedVarNames.find(
    r => r.toLocaleLowerCase() === lowerName
  )

  if (!!reservedMatch) {
    return {
      error: `Variable name is reserved: ${reservedMatch}`,
    }
  }

  const matchingName = variables.find(
    v => v.name.toLocaleLowerCase() === lowerName
  )

  if (!!matchingName) {
    return {
      error: `Variable name must be unique`,
    }
  }

  if (!varName[0].match(/[A-Z]|[_]/i)) {
    return {
      error: `Variable name must begin with a letter or underscore`,
    }
  }

  return {error: null}
}
