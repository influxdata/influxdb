import {
  getDependencyNames,
  graphFromTemplates,
  hydrateTemplates,
  newTemplateValues,
  topologicalSort,
} from 'src/tempVars/utils/graph'

import {TemplateType, TemplateValueType} from 'src/types'

function expectInOrder(xs, a, b) {
  expect(xs.indexOf(a)).toBeLessThan(xs.indexOf(b))
}

describe('getDependencyNames', () => {
  test('can extract template dependency names from query backed template', () => {
    const template = {
      id: '',
      label: '',
      tempVar: '',
      type: TemplateType.MetaQuery,
      query: {
        influxql: `SELECT :foo: FROM :bar: WHERE :baz: ~= /:yo:/`,
      },
      values: [],
    }

    const expected = [':foo:', ':bar:', ':baz:', ':yo:']
    const actual = getDependencyNames(template)

    expect(new Set(actual)).toEqual(new Set(expected))
  })

  test('can extract template dependency names from a non-query backed template', () => {
    const template = {
      id: '',
      label: '',
      tempVar: '',
      type: TemplateType.CSV,
      query: {},
      values: [
        {
          value: ':function:(:fieldKey:)',
          type: TemplateValueType.CSV,
          selected: true,
          localSelected: false,
        },
        {
          value: ':function:(mean(:fieldKey:))',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: true,
        },
      ],
    }

    const expected = [':function:', ':fieldKey:']
    const actual = getDependencyNames(template)

    expect(new Set(actual)).toEqual(new Set(expected))
  })
})

describe('topologicalSort', () => {
  test('can sort a simple graph', () => {
    // Attempt to sort the following graph:
    //
    //     a +---> b
    //       |
    //       +---> c +---> d +---> e
    //
    const templates = [
      {
        id: 'a',
        label: '',
        tempVar: ':a:',
        type: TemplateType.MetaQuery,
        query: {
          influxql: ':b: :c:',
        },
        values: [],
      },
      {
        id: 'b',
        label: '',
        tempVar: ':b:',
        type: TemplateType.MetaQuery,
        query: {
          influxql: '',
        },
        values: [],
      },
      {
        id: 'c',
        label: '',
        tempVar: ':c:',
        type: TemplateType.MetaQuery,
        query: {
          influxql: ':d:',
        },
        values: [],
      },
      {
        id: 'd',
        label: '',
        tempVar: ':d:',
        type: TemplateType.MetaQuery,
        query: {
          influxql: ':e:',
        },
        values: [],
      },
      {
        id: 'e',
        label: '',
        tempVar: ':e:',
        type: TemplateType.MetaQuery,
        query: {
          influxql: '',
        },
        values: [],
      },
    ]

    const graph = graphFromTemplates(templates)
    const a = graph.find(n => n.initialTemplate.id === 'a')
    const b = graph.find(n => n.initialTemplate.id === 'b')
    const c = graph.find(n => n.initialTemplate.id === 'c')
    const d = graph.find(n => n.initialTemplate.id === 'd')
    const e = graph.find(n => n.initialTemplate.id === 'e')

    const sorted = topologicalSort(graph)

    expectInOrder(sorted, a, b)
    expectInOrder(sorted, a, c)
    expectInOrder(sorted, c, d)
    expectInOrder(sorted, d, e)
  })
})

describe('graphFromTemplates', () => {
  test('can construct a graph with deep branches, isolated nodes, and diamonds', () => {
    // Attempt to construct the following graph:
    //
    //     a +-----> b
    //       |
    //       +-----> c +---> d
    //
    //               e
    //
    //     f +-----> g +---> i
    //       |
    //       |               ^
    //       |               |
    //       +-----> h +-----+
    //
    const templates = [
      {
        id: 'a',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':a:',
        query: {
          influxql: ':b: :c:',
        },
        values: [],
      },
      {
        id: 'b',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':b:',
        query: {
          influxql: '',
        },
        values: [],
      },
      {
        id: 'c',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':c:',
        query: {
          influxql: ':d:',
        },
        values: [],
      },
      {
        id: 'd',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':d:',
        query: {
          influxql: '',
        },
        values: [],
      },
      {
        id: 'e',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':e:',
        query: {
          influxql: '',
        },
        values: [],
      },
      {
        id: 'f',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':f:',
        query: {
          influxql: ':g: :h:',
        },
        values: [],
      },
      {
        id: 'g',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':g:',
        query: {
          influxql: ':i:',
        },
        values: [],
      },
      {
        id: 'h',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':h:',
        query: {
          influxql: ':i:',
        },
        values: [],
      },
      {
        id: 'i',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':i:',
        query: {
          influxql: '',
        },
        values: [],
      },
    ]

    const graph = graphFromTemplates(templates)
    const a = graph.find(n => n.initialTemplate === templates[0])
    const b = graph.find(n => n.initialTemplate === templates[1])
    const c = graph.find(n => n.initialTemplate === templates[2])
    const d = graph.find(n => n.initialTemplate === templates[3])
    const e = graph.find(n => n.initialTemplate === templates[4])
    const f = graph.find(n => n.initialTemplate === templates[5])
    const g = graph.find(n => n.initialTemplate === templates[6])
    const h = graph.find(n => n.initialTemplate === templates[7])
    const i = graph.find(n => n.initialTemplate === templates[8])

    expect(new Set(a.parents)).toEqual(new Set([]))
    expect(new Set(a.children)).toEqual(new Set([b, c]))

    expect(new Set(b.parents)).toEqual(new Set([a]))
    expect(new Set(b.children)).toEqual(new Set([]))

    expect(new Set(c.parents)).toEqual(new Set([a]))
    expect(new Set(c.children)).toEqual(new Set([d]))

    expect(new Set(d.parents)).toEqual(new Set([c]))
    expect(new Set(d.children)).toEqual(new Set([]))

    expect(new Set(e.parents)).toEqual(new Set([]))
    expect(new Set(e.children)).toEqual(new Set([]))

    expect(new Set(f.parents)).toEqual(new Set([]))
    expect(new Set(f.children)).toEqual(new Set([g, h]))

    expect(new Set(g.parents)).toEqual(new Set([f]))
    expect(new Set(g.children)).toEqual(new Set([i]))

    expect(new Set(h.parents)).toEqual(new Set([f]))
    expect(new Set(h.children)).toEqual(new Set([i]))

    expect(new Set(i.parents)).toEqual(new Set([g, h]))
    expect(new Set(i.children)).toEqual(new Set())
  })

  test('throws an error if attempting to create graph with cycles', () => {
    const templates = [
      {
        id: 'a',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':a:',
        query: {
          influxql: ':b:',
        },
        values: [],
      },
      {
        id: 'b',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':b:',
        query: {
          influxql: ':a:',
        },
        values: [],
      },
    ]

    expect(() => graphFromTemplates(templates)).toThrow(/cyclic/)
  })
})

describe('newTemplateValues', () => {
  test('uses supplied value for localSelectedValue if exists', () => {
    const template = {
      id: '0',
      tempVar: ':a:',
      label: '',
      query: {},
      type: TemplateType.CSV,
      values: [
        {
          value: 'a',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
        {
          value: 'b',
          type: TemplateValueType.CSV,
          selected: true,
          localSelected: false,
        },
        {
          value: 'c',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
      ],
    }

    const result = newTemplateValues(template, ['a', 'b', 'c'], 'c')

    expect(result).toEqual([
      {
        value: 'a',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: false,
      },
      {
        value: 'b',
        type: TemplateValueType.CSV,
        selected: true,
        localSelected: false,
      },
      {
        value: 'c',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: true,
      },
    ])
  })

  test('uses existing localSelectedValue if none supplied', () => {
    const template = {
      id: '0',
      tempVar: ':a:',
      label: '',
      query: {},
      type: TemplateType.CSV,
      values: [
        {
          value: 'a',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
        {
          value: 'b',
          type: TemplateValueType.CSV,
          selected: true,
          localSelected: true,
        },
        {
          value: 'c',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
      ],
    }

    const result = newTemplateValues(template, ['a', 'b', 'c'])

    expect(result).toEqual([
      {
        value: 'a',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: false,
      },
      {
        value: 'b',
        type: TemplateValueType.CSV,
        selected: true,
        localSelected: true,
      },
      {
        value: 'c',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: false,
      },
    ])
  })

  test('defaults to selected value if no localSelectedValue and no supplied value', () => {
    const template = {
      id: '0',
      tempVar: ':a:',
      label: '',
      query: {},
      type: TemplateType.CSV,
      values: [
        {
          value: 'a',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
        {
          value: 'b',
          type: TemplateValueType.CSV,
          selected: true,
          localSelected: false,
        },
        {
          value: 'c',
          type: TemplateValueType.CSV,
          selected: false,
          localSelected: false,
        },
      ],
    }

    const result = newTemplateValues(template, ['a', 'b', 'c'])

    expect(result).toEqual([
      {
        value: 'a',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: false,
      },
      {
        value: 'b',
        type: TemplateValueType.CSV,
        selected: true,
        localSelected: true,
      },
      {
        value: 'c',
        type: TemplateValueType.CSV,
        selected: false,
        localSelected: false,
      },
    ])
  })
})

describe('hydrateTemplates', () => {
  test('it can hydrate a simple template graph', async () => {
    const templates = [
      {
        id: 'a',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':a:',
        query: {
          influxql: 'query for a depending on :b: and :c:',
        },
        values: [],
      },
      {
        id: 'b',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':b:',
        query: {
          influxql: 'query for b',
        },
        values: [
          {
            value: 'selected b value',
            type: TemplateValueType.MetaQuery,
            selected: true,
            localSelected: true,
          },
        ],
      },
      {
        id: 'c',
        type: TemplateType.MetaQuery,
        label: '',
        tempVar: ':c:',
        query: {
          influxql: 'query for c',
        },
        values: [
          {
            value: 'selected c value',
            type: TemplateValueType.MetaQuery,
            selected: true,
            localSelected: true,
          },
        ],
      },
    ]

    const fakeFetcher = {
      fetch(query) {
        const results = {
          'query for b': ['selected b value'],
          'query for c': ['selected c value'],
          'query for a depending on selected b value and selected c value': [
            'success',
          ],
        }

        const queryResults = results[query]

        if (!queryResults) {
          throw new Error('Ran unexpected query')
        }

        return Promise.resolve(queryResults)
      },
    }

    const result = await hydrateTemplates(templates, {fetcher: fakeFetcher})

    expect(result.find(t => t.id === 'a').values).toContainEqual({
      value: 'success',
      type: TemplateValueType.MetaQuery,
      selected: true,
      localSelected: true,
    })
  })
})
