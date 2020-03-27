// Utils
import {ValueFetcher} from 'src/variables/utils/ValueFetcher'
import {hydrateVars} from 'src/variables/utils/hydrateVars'

// Mocks
import {createVariable} from 'src/variables/mocks'

// Types
import {Variable, CancellationError, RemoteDataState} from 'src/types'

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

    const b: Variable = {
      id: 'b',
      name: 'b',
      orgID: '',
      labels: [],
      arguments: {
        type: 'map',
        values: {
          k: 'v',
        },
      },
      status: RemoteDataState.NotStarted,
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
    // appropriate substitution is actually taking place
    expect(actual.a.error).toBeFalsy()
    expect(actual.b.error).toBeFalsy()
  })

  test('works with constant template variables', async () => {
    const a = createVariable('a', 'f(x: v.b)')

    const b: Variable = {
      id: 'b',
      name: 'b',
      orgID: '',
      labels: [],
      arguments: {
        type: 'constant',
        values: ['v1', 'v2'],
      },
      status: RemoteDataState.NotStarted,
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
