// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ConnectionInformation from 'src/onboarding/components/configureStep/streaming/ConnectionInformation'

// Types
import {RemoteDataState} from 'src/types'

const setup = (override = {}) => {
  const props = {
    loading: RemoteDataState.NotStarted,
    bucket: 'defbuck',
    ...override,
  }

  const wrapper = shallow(<ConnectionInformation {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConnectionInformation', () => {
  it('renders', () => {
    const {wrapper} = setup()

    expect(wrapper.exists()).toBe(true)
  })

  it('matches snapshot if loading', () => {
    const {wrapper} = setup({loading: RemoteDataState.Loading})

    expect(wrapper).toMatchSnapshot()
  })

  it('matches snapshot if success', () => {
    const {wrapper} = setup({loading: RemoteDataState.Done})

    expect(wrapper).toMatchSnapshot()
  })

  it('matches snapshot if error', () => {
    const {wrapper} = setup({loading: RemoteDataState.Error})

    expect(wrapper).toMatchSnapshot()
  })
})
