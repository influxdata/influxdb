import _ from 'lodash'
import {
  fieldWalk,
  removeField,
  getFieldsDeep,
  fieldNamesDeep,
} from 'shared/reducers/helpers/fields'

describe('Reducers.Helpers.Fields', () => {
  it('can walk all fields and get all values', () => {
    const fields = [
      {
        value: 'fn1',
        type: 'func',
        args: [
          {value: 'f1', type: 'func', args: [{value: 'f2', type: 'field'}]},
        ],
      },
      {value: 'fn1', type: 'func', args: [{value: 'f2', type: 'field'}]},
      {value: 'fn2', type: 'func', args: [{value: 'f2', type: 'field'}]},
    ]
    const actual = fieldWalk(fields, f => _.get(f, 'value'))
    expect(actual).to.deep.equal(['fn1', 'f1', 'f2', 'fn1', 'f2', 'fn2', 'f2'])
  })

  it('can return all unique fields for type field', () => {
    const fields = [
      {
        value: 'fn1',
        type: 'func',
        args: [
          {value: 'f1', type: 'func', args: [{value: 'f2', type: 'field'}]},
        ],
      },
      {value: 'fn1', type: 'func', args: [{value: 'f2', type: 'field'}]},
      {value: 'fn2', type: 'func', args: [{value: 'f2', type: 'field'}]},
    ]
    const actual = getFieldsDeep(fields)
    expect(actual).to.deep.equal([{value: 'f2', type: 'field'}])
  })

  it('can return all unique field value for type field', () => {
    const fields = [
      {
        value: 'fn1',
        type: 'func',
        args: [
          {value: 'f1', type: 'func', args: [{value: 'f2', type: 'field'}]},
        ],
      },
      {value: 'fn1', type: 'func', args: [{value: 'f2', type: 'field'}]},
      {value: 'fn2', type: 'func', args: [{value: 'f2', type: 'field'}]},
    ]
    const actual = fieldNamesDeep(fields)
    expect(actual).to.deep.equal(['f2'])
  })

  describe('removeField', () => {
    it('can remove fields at any level of the tree', () => {
      const fields = [
        {
          value: 'fn1',
          type: 'func',
          args: [
            {value: 'f1', type: 'func', args: [{value: 'f2', type: 'field'}]},
          ],
        },
        {value: 'fn2', type: 'func', args: [{value: 'f2', type: 'field'}]},
        {value: 'fn3', type: 'func', args: [{value: 'f3', type: 'field'}]},
      ]
      const actual = removeField('f2', fields)
      expect(actual).to.deep.equal([
        {value: 'fn3', type: 'func', args: [{value: 'f3', type: 'field'}]},
      ])
    })

    it('can remove fields from a flat field list', () => {
      const fields = [
        {value: 'f1', type: 'field'},
        {value: 'f2', type: 'field'},
      ]
      const actual = removeField('f2', fields)
      expect(actual).to.deep.equal([{value: 'f1', type: 'field'}])
    })
  })
})
