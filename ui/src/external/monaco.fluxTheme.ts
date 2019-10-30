import initialize from 'src/external/monaco'

initialize().then((monaco: any) => {
  monaco.editor.defineTheme('fluxTheme', {
    base: 'vs',
    inherit: false,
    rules: [{token: 'keyword', foreground: 'ff00ff', fontStyle: 'bold'}],
  })
})
