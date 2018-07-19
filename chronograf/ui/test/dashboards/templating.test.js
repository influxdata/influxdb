import {applyMasks, insertTempVar, unMask} from 'src/tempVars/constants'

const masquerade = query => {
  const masked = applyMasks(query)
  const inserted = insertTempVar(masked, ':REPLACED:')
  const unmasked = unMask(inserted)
  return unmasked
}

describe('template varmoji', () => {
  it('replaced incomplete templates', () => {
    const query = 'select :ONE from db1'
    const expected = 'select :REPLACED: from db1'
    expect(masquerade(query)).toBe(expected)
  })

  it('replaced incomplete templates with dashes', () => {
    const query = 'select :ONE-TWO from db1'
    const expected = 'select :REPLACED: from db1'
    expect(masquerade(query)).toBe(expected)
  })

  it('ignores whole templates while replacing incomplete templates', () => {
    const query = 'select :ONE-TWO::THREE from db1'
    const expected = 'select :ONE-TWO::REPLACED: from db1'
    expect(masquerade(query)).toBe(expected)
  })

  it('replaces incomplete templates anywhere in the string', () => {
    const query = 'select "foo" from db1 WHERE time > :ONE'
    const expected = 'select "foo" from db1 WHERE time > :REPLACED:'
    expect(masquerade(query)).toBe(expected)
  })
})
