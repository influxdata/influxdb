import {getReadBuckets} from './getReadBuckets'

// These tests are skipped until we can use WASM modules in Jest
describe.skip('getReadBuckets', () => {
  test('handles an empty script', () => {
    expect(getReadBuckets('')).toEqual([])
  })

  test('can find buckets read from in a Flux query', () => {
    const script = `
from(bucket:"foo") |> limit(limit:100, offset:10)

  |> filter(fn: (r) => r.foo and r.bar or r.buz)                from
  
  (bucket:
  
  "bar"
)

|> foo()
|> fromSnozzles(bucket: "moo")
|> moo()

|> to( bucket: "baz" )
`.trim()

    expect(getReadBuckets(script)).toEqual(['foo', 'bar'])
  })
})
