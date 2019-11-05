export const addFluxTheme = monaco => {
  monaco.editor.defineTheme('fluxTheme', {
    base: 'vs',
    inherit: false,
    rules: [{token: 'keyword', foreground: 'ff00ff', fontStyle: 'bold'}],
  })
}
