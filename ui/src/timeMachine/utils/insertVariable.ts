import {Position} from 'codemirror'

const rejoinScript = (scriptLines: string[]): string => {
  return scriptLines.join('\n')
}

const getCursorPosition = (
  currentLineNumber: number,
  currentCharacterNumber: number,
  variableName: string
) => {
  return {
    line: currentLineNumber,
    ch: currentCharacterNumber + formatVariable(variableName).length,
  }
}

const insertAtCharacter = (
  lineNumber: number,
  characterNumber: number,
  scriptLines: string[],
  variableName: string
): string => {
  const lineToEdit = scriptLines[lineNumber]
  const front = lineToEdit.slice(0, characterNumber)
  const back = lineToEdit.slice(characterNumber, lineToEdit.length)

  const updatedLine = `${front}${formatVariable(variableName)}${back}`
  scriptLines[lineNumber] = updatedLine

  return rejoinScript(scriptLines)
}

const formatVariable = (variableName: string): string => {
  return `v.${variableName}`
}

export const insertVariable = (
  currentLineNumber: number,
  currentCharacterNumber: number,
  currentScript: string,
  variableName: string
): {updatedScript: string; cursorPosition: Position} => {
  const scriptLines = currentScript.split('\n')

  const updatedScript = insertAtCharacter(
    currentLineNumber,
    currentCharacterNumber,
    scriptLines,
    variableName
  )

  const updatedCursorPosition = getCursorPosition(
    currentLineNumber,
    currentCharacterNumber,
    variableName
  )

  return {updatedScript, cursorPosition: updatedCursorPosition}
}
