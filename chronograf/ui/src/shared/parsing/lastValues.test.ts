import lastValues from 'src/shared/parsing/lastValues'

describe('lastValues', () => {
  it('returns the correct value when response is empty', () => {
    expect(lastValues([])).toEqual({
      lastValues: [''],
      series: [''],
    })
  })
})
