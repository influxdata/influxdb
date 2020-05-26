import {
  getUserVariableNames,
  getVariables,
  getAllVariables,
  getVariable,
} from 'src/variables/selectors'
import {AppState} from 'src/types'

const MOCKSTATE = ({
  app: {
    persisted: {
      timeZone: 'UTC',
    },
  },
  currentDashboard: {
    id: '',
  },
  resources: {
    variables: {
      byID: {
        '1234': {
          name: '1234',
          selected: ['abc'],
          arguments: {
            type: 'constant',
            values: ['abc', 'def', 'ghi'],
          },
        },
        '5678': {
          name: '5678',
          selected: ['abc'],
          arguments: {
            type: 'constant',
            values: ['abc', 'def', 'ghi'],
          },
        },
      },
      allIDs: ['5678', '1234'],
      values: {
        qwerty: {
          values: {
            '1234': {
              selected: ['def'],
            },
          },
          order: ['1234', '5678'],
        },
        coleman: {
          values: {
            '1234': {
              selected: ['ghi'],
            },
          },
        },
      },
    },
  },
} as any) as AppState

describe('VariableSelectors', () => {
  describe('getVariable', () => {
    it('should grab a variable', () => {
      const vardawg = getVariable(MOCKSTATE, '1234')

      expect(vardawg.selected[0]).toEqual('abc')
      expect(vardawg.arguments.type).toEqual('constant')
      expect(vardawg.arguments.values.length).toEqual(3)
    })

    it('should hydrate a user selected option', () => {
      const vardawg = getVariable(
        {
          ...MOCKSTATE,
          currentDashboard: {
            id: 'qwerty',
          },
        },
        '1234'
      )

      expect(vardawg.selected[0]).toEqual('def')
    })

    it('should contain a selection to a context', () => {
      const state = {
        ...MOCKSTATE,
        currentDashboard: {
          id: 'coleman',
        },
      }
      const vardawg = getVariable(state, '1234')
      const vardawgReturns = getVariable(state, '5678')

      expect(vardawg.selected[0]).toEqual('ghi')
      expect(vardawgReturns.selected[0]).toEqual('abc')
    })
  })

  describe('getVariables', () => {
    it('should load all user vars in the default order', () => {
      const vars = getVariables(MOCKSTATE)

      expect(vars.length).toEqual(2)
      expect(vars[0].name).toEqual('5678')
      expect(vars[1].name).toEqual('1234')
    })

    it("should load all user vars in a context's order", () => {
      const state = {
        ...MOCKSTATE,
        currentDashboard: {
          id: 'qwerty',
        },
      }
      const vars = getVariables(state)

      expect(vars.length).toEqual(2)
      expect(vars[0].name).toEqual('1234')
      expect(vars[1].name).toEqual('5678')
    })

    it('should roll over to the default order', () => {
      const state = {
        ...MOCKSTATE,
        currentDashboard: {
          id: 'coleman',
        },
      }
      const vars = getVariables(state)

      expect(vars.length).toEqual(2)
      expect(vars[0].name).toEqual('5678')
      expect(vars[1].name).toEqual('1234')
    })
  })

  describe('getAllVariables', () => {
    it('should load all user vars in the default order', () => {
      const vars = getAllVariables(MOCKSTATE)

      expect(vars.length).toEqual(4)
      expect(vars[0].name).toEqual('5678')
      expect(vars[1].name).toEqual('1234')
    })

    it("should load all user vars in a context's order", () => {
      const state = {
        ...MOCKSTATE,
        currentDashboard: {
          id: 'qwerty',
        },
      }
      const vars = getAllVariables(state)

      expect(vars.length).toEqual(4)
      expect(vars[0].name).toEqual('1234')
      expect(vars[1].name).toEqual('5678')
    })

    it('should respect chaos', () => {
      const vars = getUserVariableNames(
        ({
          currentDashboard: {
            id: 'qwerty',
          },
          resources: {
            variables: {
              allIDs: ['5678', '1234', 'abc'],
              values: {
                qwerty: {
                  order: ['ghi', '1234', '5678', 'def'],
                },
              },
            },
          },
        } as any) as AppState,
        'qwerty'
      )

      expect(vars.length).toEqual(3)
      expect(vars[0]).toEqual('1234')
      expect(vars[1]).toEqual('5678')
      expect(vars[2]).toEqual('abc')
    })

    // skipping this one as it requires some weird mocking
    // to be done for the ast parser
    it.skip('should dynamically load in the windowPeriod', () => {
      const vars = getAllVariables(
        ({
          ...MOCKSTATE,
          ...{
            timeMachines: {
              activeTimeMachineID: '12',
              timeMachines: {
                '12': {
                  activeQueryIndex: 0,
                  draftQueries: [
                    {
                      text: `
from(bucket: "project")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "docker_container_cpu")
  |> filter(fn: (r) => r._field == "usage_percent")
  |> aggregateWindow(every: 1m, fn: max)
  |> yield(name: "max")
                    `,
                      hidden: false,
                    },
                  ],
                },
              },
            },
          },
        } as any) as AppState,
        ''
      )

      expect(vars.length).toEqual(5)
    })
  })
})
