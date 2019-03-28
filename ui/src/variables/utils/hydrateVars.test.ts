// Utils
import {ValueFetcher} from 'src/variables/utils/ValueFetcher'
import {hydrateVars, exportVariables} from 'src/variables/utils/hydrateVars'

// Types
import {Variable} from '@influxdata/influx'
import {CancellationError} from 'src/types/promises'

const createVariable = (
  name: string,
  query: string,
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  arguments: {
    type: 'query',
    values: {
      query,
      language: 'flux',
    },
  },
})

class FakeFetcher implements ValueFetcher {
  responses = {}

  fetch(_, __, query, ___, ____, _____) {
    return {
      cancel: () => {},
      promise: this.responses[query],
    }
  }

  setResponse(variable, response) {
    this.responses[variable.arguments.values.query] = response
  }
}

describe('hydrate vars', () => {
  describe('exportVariables', () => {
    test('should find variable exports', () => {
      const a = createVariable('a', 'f(x: v.b, v.c)')
      const b = createVariable('b', 'beep')
      const c = createVariable('c', 'robit')
      const d = createVariable('d', 'nooooo!')
      const vars = [a, b, c, d]

      const actual = exportVariables([a], vars)

      expect(actual).toEqual([a, b, c])
    })
  })

  test('should invalidate cyclic subgraphs', async () => {
    // Construct the following graph:
    //
    //     digraph {
    //       a -> b
    //       b -> c
    //       c -> d
    //       d -> e
    //       d -> b
    //       f -> g
    //     }
    //
    const a = createVariable('a', 'f(x: v.b)')
    const b = createVariable('b', 'f(x: v.c)')
    const c = createVariable('c', 'f(x: v.d)')
    const d = createVariable('d', 'f(x: v.b, y: v.e)')
    const e = createVariable('e', 'f() // called by f')
    const f = createVariable('f', 'f(x: v.g)')
    const g = createVariable('g', 'f() // called by g')
    const vars = [a, b, c, d, e, f, g]

    const fetcher = new FakeFetcher()

    fetcher.setResponse(
      e,
      Promise.resolve({
        values: ['eVal'],
        valueType: 'string',
        selectedValue: 'eVal',
      })
    )

    fetcher.setResponse(
      g,
      Promise.resolve({
        values: ['gVal'],
        valueType: 'string',
        selectedValue: 'gVal',
      })
    )

    fetcher.setResponse(
      f,
      Promise.resolve({
        values: ['fVal'],
        valueType: 'string',
        selectedValue: 'fVal',
      })
    )

    const actual = await hydrateVars(vars, vars, {
      url: '',
      orgID: '',
      selections: {},
      fetcher,
    }).promise

    // We expect the end state of the graph to be:
    //
    //     digraph {
    //       a -> b
    //       b -> c
    //       c -> d
    //       d -> e
    //       d -> b
    //       f -> g
    //       a [fontcolor = "red"]
    //       b [fontcolor = "red"]
    //       c [fontcolor = "red"]
    //       d [fontcolor = "red"]
    //       e [fontcolor = "green"]
    //       f [fontcolor = "green"]
    //       g [fontcolor = "green"]
    //     }
    //
    expect(actual.a.error).toBeTruthy()
    expect(actual.b.error).toBeTruthy()
    expect(actual.c.error).toBeTruthy()
    expect(actual.d.error).toBeTruthy()

    expect(actual.e.error).toBeFalsy()
    expect(actual.f.error).toBeFalsy()
    expect(actual.g.error).toBeFalsy()

    expect(actual.e).toEqual({
      values: ['eVal'],
      valueType: 'string',
      selectedValue: 'eVal',
    })

    expect(actual.g).toEqual({
      values: ['gVal'],
      valueType: 'string',
      selectedValue: 'gVal',
    })

    expect(actual.f).toEqual({
      values: ['fVal'],
      valueType: 'string',
      selectedValue: 'fVal',
    })
  })

  test('should invalidate all ancestors of a node when it fails', async () => {
    // Construct the following graph:
    //
    //     digraph {
    //       a -> b
    //       b -> c
    //     }
    //
    // and then make hydrating `b` fail.
    const a = createVariable('a', 'f(x: v.b)')
    const b = createVariable('b', 'f(x: v.c)')
    const c = createVariable('c', 'f()')
    const vars = [a, b, c]

    const fetcher = new FakeFetcher()

    fetcher.setResponse(
      c,
      Promise.resolve({
        values: ['cVal'],
        valueType: 'string',
        selectedValue: 'cVal',
      })
    )

    fetcher.setResponse(b, Promise.reject(new Error('oopsy whoopsies')))

    const actual = await hydrateVars(vars, vars, {
      url: '',
      orgID: '',
      selections: {},
      fetcher,
    }).promise

    // We expect the following end state:
    //
    //     digraph {
    //       a -> b
    //       b -> c
    //       a [fontcolor = "red"]
    //       b [fontcolor = "red"]
    //       c [fontcolor = "green"]
    //     }
    //
    expect(actual.a.error).toBeTruthy()
    expect(actual.b.error).toBeTruthy()
    expect(actual.c.error).toBeFalsy()
  })

  test('works with map template variables', async () => {
    const a = createVariable('a', 'f(x: v.b)')

    const b = {
      id: 'b',
      name: 'b',
      orgID: '',
      arguments: {
        type: 'map',
        values: {
          k: 'v',
        },
      },
    }

    const vars = [a, b]

    const fetcher = new FakeFetcher()

    fetcher.setResponse(
      a,
      Promise.resolve({
        values: ['aVal'],
        valueType: 'string',
        selectedValue: 'aVal',
      })
    )

    const actual = await hydrateVars(vars, vars, {
      url: '',
      orgID: '',
      selections: {},
      fetcher,
    }).promise

    // Basic test for now, we would need an icky mock to assert that the
    // approriate substitution is actually taking place
    expect(actual.a.error).toBeFalsy()
    expect(actual.b.error).toBeFalsy()
  })

  test('works with constant template variables', async () => {
    const a = createVariable('a', 'f(x: v.b)')

    const b = {
      id: 'b',
      name: 'b',
      orgID: '',
      arguments: {
        type: 'constant',
        values: ['v1', 'v2'],
      },
    }

    const vars = [a, b]

    const fetcher = new FakeFetcher()

    fetcher.setResponse(
      a,
      Promise.resolve({
        values: ['aVal'],
        valueType: 'string',
        selectedValue: 'aVal',
      })
    )

    const actual = await hydrateVars(vars, vars, {
      url: '',
      orgID: '',
      selections: {},
      fetcher,
    }).promise

    expect(actual.a.error).toBeFalsy()
    expect(actual.b.error).toBeFalsy()
  })

  test('should be cancellable', done => {
    expect.assertions(1)

    const a = createVariable('a', 'f()')
    const fetcher = new FakeFetcher()

    fetcher.setResponse(a, new Promise(() => {}))

    const {cancel, promise} = hydrateVars([a], [a], {
      url: '',
      orgID: '',
      selections: {},
      fetcher,
    })

    promise.catch(e => {
      expect(e).toBeInstanceOf(CancellationError)
      done()
    })

    cancel()
  })
})
