import CodeMirror from 'codemirror'
import {IInstance} from 'react-codemirror2'

import {Suggestion} from 'src/types/flux'
import {Hints} from 'src/types/codemirror'

export const getFluxCompletions = (
  editor: IInstance,
  list: Suggestion[]
): Hints => {
  const cursor = editor.getCursor()
  const currentLine = editor.getLine(cursor.line)
  const trailingWhitespace = /[\w$]+/

  let start = cursor.ch
  let end = start

  // Move end marker until a space or end of line is reached
  while (
    end < currentLine.length &&
    trailingWhitespace.test(currentLine.charAt(end))
  ) {
    end += 1
  }

  // Move start marker until a space or the beginning of line is reached
  while (start && trailingWhitespace.test(currentLine.charAt(start - 1))) {
    start -= 1
  }

  // If not completing inside a current word, return list of all possible suggestions
  if (start === end) {
    return {
      from: CodeMirror.Pos(cursor.line, start),
      to: CodeMirror.Pos(cursor.line, end),
      list: list.map(s => s.name),
    }
  }

  const currentWord = currentLine.slice(start, end)
  const listFilter = new RegExp(`^${currentWord}`, 'i')

  // Otherwise return suggestions that contain the current word as a substring
  return {
    from: CodeMirror.Pos(cursor.line, start),
    to: CodeMirror.Pos(cursor.line, end),
    list: list.filter(s => s.name.match(listFilter)).map(s => s.name),
  }
}
