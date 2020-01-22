import {MonacoType, EditorType} from 'src/types'

export const toggleCommenting = (s: string, isTogglingOn: boolean) => {
  if (isTogglingOn) {
    return `// ${s}`
  }
  return s.replace(/\/\/\s*/, '')
}

export const isCommented = (s: string) => !!s.match(/^\s*(\/\/(.*)$)/g)

export function addKeyBindings(editor: EditorType, monaco: MonacoType) {
  editor.addAction({
    // An unique identifier of the contributed action.
    id: 'toggle-comment',

    // A label of the action that will be presented to the user.
    label: 'toggling comments',

    // An optional array of keybindings for the action.
    keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.US_SLASH],

    run: function(ed: EditorType) {
      const values = ed.getValue().split('\n')

      const selection = ed.getSelection()

      const {
        startLineNumber,
        endLineNumber,
        positionColumn,
        selectionStartColumn,
      } = selection

      // if any of the lines in the selection is uncommented then toggle commenting on
      const isTogglingCommentingOn = values
        .slice(startLineNumber - 1, endLineNumber)
        .some((v: string) => !(v === '') && !isCommented(v))

      const updatedValues = values.map((v: string, i: number) =>
        i >= startLineNumber - 1 && i <= endLineNumber - 1
          ? toggleCommenting(v, isTogglingCommentingOn)
          : v
      )

      ed.setValue(updatedValues.join('\n'))

      ed.setSelection({
        ...selection,
        selectionStartColumn: isTogglingCommentingOn
          ? selectionStartColumn + 3
          : selectionStartColumn - 3,
        positionColumn: isTogglingCommentingOn
          ? positionColumn + 3
          : positionColumn - 3,
      })

      return null
    },
  })
}
