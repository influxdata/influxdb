import {lexTemplateQuery} from 'src/tempVars/utils/hydrate'

test('lexTemplateQuery', () => {
  const query = `SELECT :foo: FROM :bar: WHERE :baz: ~= /:yo:/`

  const expected = [':foo:', ':bar:', ':baz:', ':yo:']
  const actual = lexTemplateQuery(query)

  expect(actual).toEqual(expected)
})

// test('graphFromTemplates', () => {
// })
