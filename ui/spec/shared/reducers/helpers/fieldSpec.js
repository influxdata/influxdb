import _ from 'lodash'
import {
  fieldWalk,
  removeField,
  getFieldsDeep,
  fieldNamesDeep,
} from 'shared/reducers/helpers/fields'

describe('field helpers', () => {
  it('can walk all fields and get all names', () => {
    const fields = [
      {
        name: 'fn1',
        type: 'func',
        args: [{name: 'f1', type: 'func', args: [{name: 'f2', type: 'field'}]}],
      },
      {name: 'fn1', type: 'func', args: [{name: 'f2', type: 'field'}]},
      {name: 'fn2', type: 'func', args: [{name: 'f2', type: 'field'}]},
    ]
    const actual = fieldWalk(fields, f => _.get(f, 'name'))
    expect(actual).to.deep.equal(['fn1', 'f1', 'f2', 'fn1', 'f2', 'fn2', 'f2'])
  })

  it('can return all unique fields for type field', () => {
    const fields = [
      {
        name: 'fn1',
        type: 'func',
        args: [{name: 'f1', type: 'func', args: [{name: 'f2', type: 'field'}]}],
      },
      {name: 'fn1', type: 'func', args: [{name: 'f2', type: 'field'}]},
      {name: 'fn2', type: 'func', args: [{name: 'f2', type: 'field'}]},
    ]
    const actual = getFieldsDeep(fields)
    expect(actual).to.deep.equal([{name: 'f2', type: 'field'}])
  })

  it('can return all unique field names for type field', () => {
    const fields = [
      {
        name: 'fn1',
        type: 'func',
        args: [{name: 'f1', type: 'func', args: [{name: 'f2', type: 'field'}]}],
      },
      {name: 'fn1', type: 'func', args: [{name: 'f2', type: 'field'}]},
      {name: 'fn2', type: 'func', args: [{name: 'f2', type: 'field'}]},
    ]
    const actual = fieldNamesDeep(fields)
    expect(actual).to.deep.equal(['f2'])
  })

  describe('removeField', () => {
    it('can remove fields at any level of the tree', () => {
      const fields = [
        {
          name: 'fn1',
          type: 'func',
          args: [
            {name: 'f1', type: 'func', args: [{name: 'f2', type: 'field'}]},
          ],
        },
        {name: 'fn2', type: 'func', args: [{name: 'f2', type: 'field'}]},
        {name: 'fn3', type: 'func', args: [{name: 'f3', type: 'field'}]},
      ]
      const actual = removeField('f2', fields)
      expect(actual).to.deep.equal([
        {name: 'fn3', type: 'func', args: [{name: 'f3', type: 'field'}]},
      ])
    })

    it('can remove fields from a flat field list', () => {
      const fields = [{name: 'f1', type: 'field'}, {name: 'f2', type: 'field'}]
      const actual = removeField('f2', fields)
      expect(actual).to.deep.equal([{name: 'f1', type: 'field'}])
    })
  })
})
