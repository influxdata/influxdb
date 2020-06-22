import register from 'src/external/monaco.onigasm'

const LANGID = 'markdown'

register(LANGID, async () => ({
  format: 'json',
  content: await import(
    /* webpackPrefetch: 0 */ 'src/external/markdown.tmLanguage.json'
  ).then(data => {
    return JSON.stringify(data)
  }),
}))

export default LANGID
