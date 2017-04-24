import {TEMPLATE_MATCHER} from 'src/dashboards/constants'

describe('templating', () => {
  it('can match the expected strings', () => {
    const matchingStrings = [
      'SELECT :$d, "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
      'SELECT :$fasdf:, "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
      'SELECT :$fa0df:, "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
      'SELECT $fasdf, :$ FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
      'SELECT "foo", "f1" FROM "db1"."rp1"."m1" WHERE time > now() - :$',
    ]

    matchingStrings.forEach(s => {
      const result = s.match(TEMPLATE_MATCHER)
      expect(result.length).to.be.above(0)
    })
  })

  it('does not match unexpected strings', () => {
    const nonMatchingStrings = [
      'SELECT "foo", "f1" FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
      'SELECT $fasdf, $asdf FROM "db1"."rp1"."m1" WHERE time > now() - 15m',
    ]

    nonMatchingStrings.forEach(s => {
      const result = s.match(TEMPLATE_MATCHER)
      expect(result).to.equal(null)
    })
  })

  it('can replace the expected strings', () => {
    const s = 'SELECT :$fasdf: FROM "db1"."rp1"."m1"'
    const tempVar = '$tv1'
    const actual = s.replace(TEMPLATE_MATCHER, tempVar)
    const expected = `SELECT ${tempVar} FROM "db1"."rp1"."m1"`

    expect(actual).to.equal(expected)
  })

  it('can replace a string with a numeric character', () => {
    const s = 'SELECT :$fas0df: FROM "db1"."rp1"."m1"'
    const tempVar = '$tv1'
    const actual = s.replace(TEMPLATE_MATCHER, tempVar)
    const expected = `SELECT ${tempVar} FROM "db1"."rp1"."m1"`

    expect(actual).to.equal(expected)
  })

  it('can replace the expected strings that are next to ,', () => {
    const s = 'SELECT :$fasdf:, "f1" FROM "db1"."rp1"."m1"'
    const tempVar = '$tv1'
    const actual = s.replace(TEMPLATE_MATCHER, tempVar)
    const expected = `SELECT ${tempVar}, "f1" FROM "db1"."rp1"."m1"`

    expect(actual).to.equal(expected)
  })

  it('can replace the expected strings that are next to .', () => {
    const s = 'SELECT "f1" FROM "db1".:$asdf:."m1"'
    const tempVar = '$tv1'
    const actual = s.replace(TEMPLATE_MATCHER, tempVar)
    const expected = `SELECT "f1" FROM "db1".${tempVar}."m1"`

    expect(actual).to.equal(expected)
  })

  it('can does not replace other tempVars', () => {
    const s = 'SELECT $foo FROM "db1".:$asdfasd:."m1"'
    const tempVar = '$tv1'
    const actual = s.replace(TEMPLATE_MATCHER, tempVar)
    const expected = `SELECT $foo FROM "db1".${tempVar}."m1"`

    expect(actual).to.equal(expected)
  })
})
