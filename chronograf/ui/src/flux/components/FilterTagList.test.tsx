import React from 'react'
import {shallow} from 'enzyme'
import FilterTagList from 'src/flux/components/FilterTagList'
import FilterTagListItem from 'src/flux/components/FilterTagListItem'

const setup = (override?) => {
  const props = {
    db: 'telegraf',
    tags: ['cpu', '_measurement'],
    filter: [],
    func: {
      id: 'f1',
      args: [{key: 'fn', value: '(r) => true'}],
    },
    nodes: [],
    bodyID: 'b1',
    declarationID: 'd1',
    onChangeArg: () => {},
    onGenerateScript: () => {},
    ...override,
  }

  const wrapper = shallow(<FilterTagList {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Flux.Components.FilterTagList', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })

  it('renders a builder when the clause is parseable', () => {
    const override = {
      nodes: [{type: 'BooleanLiteral', source: 'true'}],
    }
    const {wrapper} = setup(override)

    const builderContents = wrapper.find(FilterTagListItem)
    expect(builderContents).not.toHaveLength(0)
  })

  it('renders a builder when the clause cannot be parsed', () => {
    const override = {
      nodes: [{type: 'Unparseable', source: 'baconcannon'}],
    }
    const {wrapper} = setup(override)

    const builderContents = wrapper.find(FilterTagListItem)
    expect(builderContents).toHaveLength(0)
  })

  describe('clause parseability', () => {
    const parser = setup().wrapper.instance() as FilterTagList

    it('recognizes a simple `true` body', () => {
      const nodes = [{type: 'BooleanLiteral', source: 'true'}]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({})
    })

    it('allows for an empty node list', () => {
      const nodes = []
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({})
    })

    it('extracts a tag condition equality', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '=='},
        {type: 'StringLiteral', source: 'tagValue'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({
        tagKey: [{key: 'tagKey', operator: '==', value: 'tagValue'}],
      })
    })

    it('extracts a tag condition inequality', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'tagValue'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({
        tagKey: [{key: 'tagKey', operator: '!=', value: 'tagValue'}],
      })
    })

    it('groups like keys together', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'value1'},
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'value2'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({
        tagKey: [
          {key: 'tagKey', operator: '!=', value: 'value1'},
          {key: 'tagKey', operator: '!=', value: 'value2'},
        ],
      })
    })

    it('separates conditions with different keys', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'key1'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'value1'},
        {type: 'MemberExpression', property: {name: 'key2'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'value2'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(true)
      expect(clause).toEqual({
        key1: [{key: 'key1', operator: '!=', value: 'value1'}],
        key2: [{key: 'key2', operator: '!=', value: 'value2'}],
      })
    })

    it('cannot recognize other operators', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '=~'},
        {type: 'StringLiteral', source: 'tagValue'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(false)
      expect(clause).toEqual({})
    })

    it('requires that operators be consistent within a key group', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '=='},
        {type: 'StringLiteral', source: 'tagValue'},
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'Operator', source: '!='},
        {type: 'StringLiteral', source: 'tagValue'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(false)
      expect(clause).toEqual({})
    })

    it('conditions must come in order to be recognizeable', () => {
      const nodes = [
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'StringLiteral', source: 'tagValue'},
        {type: 'Operator', source: '=~'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(false)
      expect(clause).toEqual({})
    })

    it('does not recognize more esoteric types', () => {
      const nodes = [
        {type: 'ArrayExpression', property: {name: 'tagKey'}},
        {type: 'MemberExpression', property: {name: 'tagKey'}},
        {type: 'StringLiteral', source: 'tagValue'},
        {type: 'Operator', source: '=~'},
      ]
      const [clause, parseable] = parser.reduceNodesToClause(nodes, [])

      expect(parseable).toBe(false)
      expect(clause).toEqual({})
    })
  })

  describe('building a filter string', () => {
    const builder = setup().wrapper.instance() as FilterTagList

    it('returns a simple filter with no conditions', () => {
      const filterString = builder.buildFilterString({})
      expect(filterString).toEqual('() => true')
    })

    it('renders a single condition', () => {
      const clause = {
        myKey: [{key: 'myKey', operator: '==', value: 'val1'}],
      }
      const filterString = builder.buildFilterString(clause)
      expect(filterString).toEqual('(r) => (r.myKey == "val1")')
    })

    it('groups like keys together', () => {
      const clause = {
        myKey: [
          {key: 'myKey', operator: '==', value: 'val1'},
          {key: 'myKey', operator: '==', value: 'val2'},
        ],
      }
      const filterString = builder.buildFilterString(clause)
      expect(filterString).toEqual(
        '(r) => (r.myKey == "val1" OR r.myKey == "val2")'
      )
    })

    it('joins conditions together with AND when operator is !=', () => {
      const clause = {
        myKey: [
          {key: 'myKey', operator: '!=', value: 'val1'},
          {key: 'myKey', operator: '!=', value: 'val2'},
        ],
      }
      const filterString = builder.buildFilterString(clause)
      expect(filterString).toEqual(
        '(r) => (r.myKey != "val1" AND r.myKey != "val2")'
      )
    })

    it('always uses AND to join conditions across keys', () => {
      const clause = {
        key1: [
          {key: 'key1', operator: '!=', value: 'val1'},
          {key: 'key1', operator: '!=', value: 'val2'},
        ],
        key2: [
          {key: 'key2', operator: '==', value: 'val3'},
          {key: 'key2', operator: '==', value: 'val4'},
        ],
      }
      const filterString = builder.buildFilterString(clause)
      expect(filterString).toEqual(
        '(r) => (r.key1 != "val1" AND r.key1 != "val2") AND (r.key2 == "val3" OR r.key2 == "val4")'
      )
    })
  })
})
