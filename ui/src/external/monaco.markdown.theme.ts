import {MonacoType} from 'src/types'

const THEME_NAME = 'markdownTheme'

function addTheme(monaco: MonacoType) {
  monaco.editor.defineTheme(THEME_NAME, {
    base: 'vs-dark',
    inherit: false,
    rules: [],
    colors: {
      'editor.foreground': '#F8F8F8',
      'editor.background': '#202028',
      'editorGutter.background': '#25252e',
      'editor.selectionBackground': '#353640',
      'editorLineNumber.foreground': '#666978',
      'editor.lineHighlightBackground': '#353640',
      'editorCursor.foreground': '#ffffff',
      'editorActiveLineNumber.foreground': '#bec2cc',
    },
  })
}

addTheme(window.monaco)

export default THEME_NAME
