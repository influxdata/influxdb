import {EditorType} from 'src/types'

export const toggleCommenting = (s: string, isTogglingOn: boolean) => {
  if (isTogglingOn) {
    return `// ${s}`
  }
  return s.replace(/\/\/\s?/, '')
}

export const isCommented = (s: string) => !!s.match(/^\s*(\/\/(.*)$)/g)

export function comments(editor: EditorType) {
  editor.addAction({
    // An unique identifier of the contributed action.
    id: 'toggle-comment',

    // A label of the action that will be presented to the user.
    label: 'toggling comments',

    // An optional array of keybindings for the action.
    keybindings: [
      window.monaco.KeyMod.CtrlCmd | window.monaco.KeyCode.US_SLASH,
    ],

    run: function run(ed) {
      const values = ed.getValue().split('\n'),
        selection = ed.getSelection(),
        {
          startLineNumber,
          endLineNumber,
          positionColumn,
          selectionStartColumn,
        } = selection,
        // if any of the lines in the selection is uncommented then toggle commenting on
        isTogglingCommentingOn = values
          .slice(startLineNumber - 1, endLineNumber)
          .some(v => !(v === '') && !isCommented(v)),
        updatedValues = values.map((v, i) => {
          if (i < startLineNumber - 1 || i > endLineNumber - 1) {
            return v
          }

          return toggleCommenting(v, isTogglingCommentingOn)
        })

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

export function submit(editor: EditorType, submitFn: () => any) {
  editor.onKeyUp(evt => {
    const {ctrlKey, code} = evt

    if (ctrlKey && code === 'Enter') {
      submitFn()
    }
  })
}
