export const THEME_NAME = 'tomlTheme'
export default function(monaco) {
  monaco.editor.defineTheme(THEME_NAME, {
    base: 'vs-dark',
    inherit: false,
    rules: [
      {
        token: 'punctuation.definition.comment.toml',
        foreground: '#676978',
      },
      {
        token: 'comment.line.number-sign.toml',
        foreground: '#676978',
      },
      {
        token: 'constant.numeric.integer.toml',
        foreground: '#7CE490',
      },
      {
        token: 'constant.numeric.float.toml',
        foreground: '#7CE490',
      },
      {
        token: 'string.quoted.double.toml',
        foreground: '#7CE490',
      },
      {
        token: 'constant.language.boolean.toml',
        foreground: '#32B08C',
      },
      {
        token: 'entity.name.section.toml',
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
