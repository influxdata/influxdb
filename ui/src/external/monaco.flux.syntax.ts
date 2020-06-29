import register from 'src/external/monaco.onigasm'

const LANGID = 'flux'

async function addSyntax() {
  await register(LANGID, async () => ({
    format: 'json',
    content: await import(
      /* webpackPrefetch: 0 */ 'src/external/flux.tmLanguage.json'
    ).then(data => {
      return JSON.stringify(data)
    }),
  }))

  window.monaco.languages.setLanguageConfiguration(LANGID, {
    autoClosingPairs: [
      {open: '"', close: '"'},
      {open: '[', close: ']'},
      {open: "'", close: "'"},
      {open: '{', close: '}'},
      {open: '(', close: ')'},
    ],
  })
}

addSyntax()

export default LANGID
