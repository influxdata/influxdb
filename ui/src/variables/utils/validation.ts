import {Variable} from '@influxdata/influx'

export const validateVariableName = (
  varName: string,
  variables: Variable[]
): {error: string} => {
  if (varName.match(/^\s*$/)) {
    return {error: 'Variable name cannot be empty'}
  }

  const matchingName = variables.find(
    v => v.name.toLocaleLowerCase() === varName.toLocaleLowerCase()
  )

  if (matchingName) {
    return {
      error: `Variable name must be unique`,
    }
  }

  return {error: null}
}
