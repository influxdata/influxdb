// Constants
import {FROM, UNION} from 'src/shared/constants/fluxFunctions'

const functionRequiresNewLine = (funcName: string): boolean => {
  switch (funcName) {
    case FROM.name:
    case UNION.name: {
      return true
    }
    default:
      return false
  }
}

export const formatFunctionForInsert = (
  funcName: string,
  fluxFunction: string,
  newLine: boolean
): string => {
  if (functionRequiresNewLine(funcName)) {
    return `\n${fluxFunction}\n`
  }

  return `${newLine ? '\n' : ''}  |> ${fluxFunction}\n`
}

export const generateImport = (
  funcPackage: string,
  script: string
): false | string => {
  const importStatement = `import "${funcPackage}"`

  if (!funcPackage || script.includes(importStatement)) {
    return false
  }

  return importStatement
}
