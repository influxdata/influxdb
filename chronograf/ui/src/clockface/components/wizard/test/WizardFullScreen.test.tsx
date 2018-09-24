import React from 'react'
import {shallow} from 'enzyme'

import WizardFullScreen from 'src/clockface/components/wizard/WizardFullScreen'
import WizardController from 'src/clockface/components/wizard/WizardController'
import SplashPage from 'src/shared/components/SplashPage'

describe('WizardFullScreen', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      children: null,
      title: undefined,
      skipLinkText: undefined,
      handleSkip: undefined,
      ...override,
    }

    return shallow(<WizardFullScreen {...props} />)
  }

  beforeEach(() => {
    jest.resetAllMocks()
    wrapper = wrapperSetup()
  })

  it('mounts without exploding', () => {
    expect(wrapper).toHaveLength(1)
  })

  it('renders one SplashPage component', () => {
    expect(wrapper.find(SplashPage)).toHaveLength(1)
  })

  it('renders no WizardController component', () => {
    expect(wrapper.find(WizardController)).toHaveLength(0)
  })

  it('matches snapshot with minimal props', () => {
    expect(wrapper).toMatchSnapshot()
  })

  describe('with children', () => {
    it('renders one WizardController component', () => {
      wrapper = wrapperSetup({children: {}})
      expect(wrapper.find(WizardController)).toHaveLength(1)
    })

    it('matches snapshot with children props', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })
})
