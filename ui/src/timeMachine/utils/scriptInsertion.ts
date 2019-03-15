import {Position} from 'codemirror'

// Constants
import {FROM, UNION} from 'src/shared/constants/fluxFunctions'

const rejoinScript = (scriptLines: string[]): string => {
  return scriptLines.join('\n')
}

const insertAtLine = (
  lineNumber: number,
  scriptLines: string[],
  textToInsert: string,
  insertOnSameLine?: boolean
): string => {
  const front = scriptLines.slice(0, lineNumber)

  const backStartIndex = insertOnSameLine ? lineNumber + 1 : lineNumber
  const back = scriptLines.slice(backStartIndex)

  const updated = [...front, textToInsert, ...back]

  return rejoinScript(updated)
}

const getInsertLineNumber = (
  currentLineNumber: number,
  scriptLines: string[]
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

export const insertFluxFunction = (
  currentLineNumber: number,
  currentScript: string,
  functionName: string,
  fluxFunction: string
): {updatedScript: string; cursorPosition: Position} => {
  const scriptLines = currentScript.split('\n')
  const insertLineNumber = getInsertLineNumber(currentLineNumber, scriptLines)
  const insertOnSameLine = currentLineNumber === insertLineNumber

  const formattedFunction = formatFunctionForInsert(functionName, fluxFunction)

  const updatedScript = insertAtLine(
    insertLineNumber,
    scriptLines,
    formattedFunction,
    insertOnSameLine
  )

  const updatedCursorPosition = getCursorPosition(
    insertLineNumber,
    formattedFunction,
    functionName
  )

  return {updatedScript, cursorPosition: updatedCursorPosition}
}
