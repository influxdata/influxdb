import register from 'src/external/monaco.onigasm'

const LANGID = 'toml'

register(LANGID, async () => ({
  format: 'json',
  content: await import(/* webpackPrefetch: 0 */ 'src/external/toml.tmLanguage.json').then(
    data => {
      return JSON.stringify(data)
    }
  ),
}))

export default LANGID
