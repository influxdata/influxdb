import AutoRefresh, {
  Props,
  OriginalProps,
} from 'src/shared/components/AutoRefresh'
import React, {Component} from 'react'
import {shallow} from 'enzyme'
import {source} from 'test/resources'

type ComponentProps = Props & OriginalProps

class MyComponent extends Component<ComponentProps> {
  public render(): JSX.Element {
    return <p>Here</p>
  }
}

const axes = {
  bounds: {
    y: [1],
    y2: [2],
  },
}

const defaultProps = {
  type: 'table',
  autoRefresh: 1,
  inView: true,
  templates: [],
  queries: [],
  axes,
  editQueryStatus: () => {},
  grabDataForDownload: () => {},
  data: [],
  isFetchingInitially: false,
  isRefreshing: false,
  queryASTs: [],
  source,
}

const setup = (overrides: Partial<ComponentProps> = {}) => {
  const ARComponent = AutoRefresh(MyComponent)

  const props = {...defaultProps, ...overrides}

  return shallow(<ARComponent {...props} />)
}

describe('Shared.Components.AutoRefresh', () => {
  describe('render', () => {
    describe('when there are no results', () => {
      it('renders the no results component', () => {
        const wrapped = setup()
        expect(wrapped.find('.graph-empty').exists()).toBe(true)
      })
    })

    describe('when there are results', () => {
      it('renderes the wrapped component', () => {
        const wrapped = setup()
        const timeSeries = [
          {
            response: {
              results: [{series: [1]}],
            },
          },
        ]
        wrapped.update()
        wrapped.setState({timeSeries})
        process.nextTick(() => {
          expect(wrapped.find(MyComponent).exists()).toBe(true)
        })
      })
    })
  })
})
