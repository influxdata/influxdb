import {MonacoType} from 'src/types'

const THEME_NAME = 'baseTheme'

function addTheme(monaco: MonacoType) {
  monaco.editor.defineTheme(THEME_NAME, {
    base: 'vs-dark',
    inherit: false,
    rules: [
      {
        token: 'support.function',
        foreground: '#9394FF',
      },
      {
        token: 'keyword.operator.new',
        foreground: '#9394FF',
      },
      {
        token: 'keyword.control.flux',
        foreground: '#9394FF',
      },
      {
        token: 'comment.line.double-slash',
        foreground: '#676978',
      },
      {
        token: 'string.quoted.double.flux',
        foreground: '#7CE490',
      },
      {
        token: 'string.regexp',
        foreground: '#FFB6A0',
      },
      {
        token: 'constant.time',
        foreground: '#6BDFFF',
      },
      {
        token: 'constant.numeric',
        foreground: '#6BDFFF',
      },
      {
        token: 'constant.language',
        foreground: '#32B08C',
      },
      {
        token: 'keyword.operator',
        foreground: '#ff4d96',
      },
      {
        token: '',
        foreground: '#f8f8f8',
        background: '#202028',
      },
    ],
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
