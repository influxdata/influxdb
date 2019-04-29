import {Position} from 'codemirror'

// Constants
import {FROM, UNION} from 'src/shared/constants/fluxFunctions'

// Types
import {FluxToolbarFunction} from 'src/types'

const rejoinScript = (scriptLines: string[]): string => {
  return scriptLines.join('\n')
}

const insertAtLine = (
  lineNumber: number,
  scriptLines: string[],
  prevScriptLines: string[],
  textToInsert: string,
  insertOnSameLine?: boolean
): string => {
  const numPrevPack = prevScriptLines.filter(sl => sl.includes('import "'))
    .length
  const numPackages = scriptLines.filter(sl => sl.includes('import "')).length
  const packChange = numPrevPack !== numPackages

  let ln = lineNumber
  let backStartIndex = insertOnSameLine ? ln + 1 : ln

  if (packChange) {
    ln = ln + numPackages
    backStartIndex = ln + numPackages
  }

  const front = scriptLines.slice(0, ln)
  const back = scriptLines.slice(backStartIndex)

  const updated = [...front, textToInsert, ...back]

  return rejoinScript(updated)
}

const getInsertLineNumber = (
  currentLineNumber: number,
  nextLines: string[]
): number => {
  const currentLine = scriptLines[currentLineNumber]

  // Insert on the current line if its an empty line
  if (!currentLine.trim()) {
    return currentLineNumber
  }

  return currentLineNumber + 1
}

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

const formatFunctionForInsert = (funcName: string, fluxFunction: string) => {
  if (functionRequiresNewLine(funcName)) {
    return `\n${fluxFunction}`
  }

  return `  |> ${fluxFunction}`
}

const getCursorPosition = (
  insertLineNumber,
  formattedFunction,
  funcName
): Position => {
  const ch = formattedFunction.length - 1
  const line = functionRequiresNewLine(funcName)
    ? insertLineNumber + 1
    : insertLineNumber

  return {line, ch}
}

const addImports = (script: string, funcPackage: string) => {
  const importStatement = `import "${funcPackage}"`

  if (!funcPackage || script.includes(importStatement)) {
    return script
  }

  return `${importStatement}\n${script}`
}

export const insertFluxFunction = (
  currentLineNumber: number,
  currentScript: string,
  func: FluxToolbarFunction
): {updatedScript: string; cursorPosition: Position} => {
  const {name, example} = func

  let updatedScript = addImports(currentScript, func.package)

  const prevScriptLines = currentScript.split('\n')
  const scriptLines = updatedScript.split('\n')

  const insertLineNumber = getInsertLineNumber(currentLineNumber, scriptLines)

  const insertOnSameLine = currentLineNumber === insertLineNumber

  const formattedFunction = formatFunctionForInsert(name, example)

  updatedScript = insertAtLine(
    insertLineNumber,
    scriptLines,
    prevScriptLines,
    formattedFunction,
    insertOnSameLine
  )

  const updatedCursorPosition = getCursorPosition(
    insertLineNumber,
    formattedFunction,
    name
  )

  return {updatedScript, cursorPosition: updatedCursorPosition}
}
