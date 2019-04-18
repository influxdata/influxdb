import {csvToMap, mapToCSV} from 'src/variables/utils/mapBuilder'

describe('MapVars', () => {
  describe('csvToMap', () => {
    it('can parse key values', () => {
      const csv = 'a,1\nb,2\nc,3\n'

      const {values: actual} = csvToMap(csv)
      expect(actual).toEqual({
        a: '1',
        b: '2',
        c: '3',
      })
    })

    it('records invalid keys', () => {
      const csv = 'a,1,2\nb,2\nc,3\n'

      const {values: actual, errors} = csvToMap(csv)
      expect(actual).toEqual({
        b: '2',
        c: '3',
      })

      expect(errors).toEqual(['a'])
    })

    it('can parse single quoted values', () => {
      const csv = `a,'1'\nb,'2'\nc,'3'\n`

      const {values: actual} = csvToMap(csv)
      expect(actual).toEqual({
        a: `'1'`,
        b: `'2'`,
        c: `'3'`,
      })
    })

    it('can parse single quoted values with commas and spaces', () => {
      const csv = `a,"'1, 2'"\nb,"'2, 3'"\nc,"'3, 4'"\n`

      const {values: actual} = csvToMap(csv)
      expect(actual).toEqual({
        a: `'1, 2'`,
        b: `'2, 3'`,
        c: `'3, 4'`,
      })
    })

    it('can parse double quoted values', () => {
      const csv = `a,"1"\nb,"2"\nc,"3"\n`

      const {values: actual} = csvToMap(csv)
      expect(actual).toEqual({
        a: '1',
        b: '2',
        c: '3',
      })
    })

    it('can parse double quoted values with commas', () => {
      const csv = `a,"1, 2"\nb,"2, 3"\nc,"3, 4"\n`

      const {values: actual} = csvToMap(csv)

      expect(actual).toEqual({
        a: '1, 2',
        b: '2, 3',
        c: '3, 4',
      })
    })
  })

  describe('mapToCSV', () => {
    it('can create a CSV', () => {
      const actual = mapToCSV({
        a: '1',
        b: '2',
        c: '3',
      })

      const expected = 'a,"1"\nb,"2"\nc,"3"'

      expect(actual).toEqual(expected)
    })
  })

  it('can double quote single quoted values', () => {
    const actual = mapToCSV({
      a: `'1, 2'`,
      b: `'2, 3'`,
      c: `'3, 4'`,
    })

    const expected = `a,"'1, 2'"\nb,"'2, 3'"\nc,"'3, 4'"`

    expect(actual).toEqual(expected)
  })

  it('can double quote keys with CSV values', () => {
    const actual = mapToCSV({
      a: '1, 2',
      b: '2, 3',
      c: '3, 4',
    })

    const expected = `a,"1, 2"\nb,"2, 3"\nc,"3, 4"`

    expect(actual).toEqual(expected)
  })
})
