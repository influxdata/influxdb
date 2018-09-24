import React from 'react'
import {shallow} from 'enzyme'

import WizardOverlay from 'src/reusable_ui/components/wizard/WizardOverlay'
import WizardController from 'src/reusable_ui/components/wizard/WizardController'
import OverlayBody from 'src/reusable_ui/components/overlays/OverlayBody'
import OverlayContainer from 'src/reusable_ui/components/overlays/OverlayContainer'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import OverlayHeading from 'src/reusable_ui/components/overlays/OverlayHeading'

describe('WizardOverlay', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      children: null,
      visible: undefined,
      title: undefined,
      toggleVisibility: () => jest.fn(),
      resetWizardState: jest.fn(),
      skipLinkText: undefined,
      maxWidth: undefined,
      jumpStep: undefined,
      ...override,
    }

    return shallow(<WizardOverlay {...props} />)
  }

  beforeEach(() => {
    jest.resetAllMocks()
    wrapper = wrapperSetup()
  })

  it('mounts without exploding', () => {
    expect(wrapper).toHaveLength(1)
  })

  it('renders no WizardController component', () => {
    expect(wrapper.find(WizardController)).toHaveLength(0)
  })

  it('renders no OverlayTechnology component', () => {
    expect(wrapper.find(OverlayTechnology)).toHaveLength(1)
  })

  it('renders no OverlayContainer component', () => {
    expect(wrapper.find(OverlayContainer)).toHaveLength(1)
  })

  it('renders no OverlayHeading component', () => {
    expect(wrapper.find(OverlayHeading)).toHaveLength(1)
  })

  it('renders no OverlayBody component', () => {
    expect(wrapper.find(OverlayBody)).toHaveLength(1)
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
