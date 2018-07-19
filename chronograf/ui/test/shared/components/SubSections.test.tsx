import {shallow} from 'enzyme'
import React from 'react'
import SubSections from 'src/shared/components/SubSections'
import SubSectionsTab from 'src/shared/components/SubSectionsTab'

const Guava = () => {
  return <div />
}

const Mango = () => {
  return <div />
}

const Pineapple = () => {
  return <div />
}

const guavaURL = 'guava'
const mangoURL = 'mango'
const pineappleURL = 'pineapple'

const defaultProps = {
  router: {
    push: () => {},
    replace: () => {},
    go: () => {},
    goBack: () => {},
    goForward: () => {},
    setRouteLeaveHook: () => {},
    isActive: () => {},
  },
  sourceID: 'fruitstand',
  parentUrl: 'fred-the-fruit-guy',
  activeSection: guavaURL,
  sections: [
    {
      url: guavaURL,
      name: 'Guava',
      component: <Guava />,
      enabled: true,
    },
    {
      url: mangoURL,
      name: 'Mango',
      component: <Mango />,
      enabled: true,
    },
    {
      url: pineappleURL,
      name: 'Pineapple',
      component: <Pineapple />,
      enabled: false,
    },
  ],
}

const setup = (override?: {}) => {
  const props = {
    ...defaultProps,
    ...override,
  }

  return shallow(<SubSections {...props} />)
}

describe('SubSections', () => {
  describe('render', () => {
    it('renders the currently active tab', () => {
      const wrapper = setup()
      const content = wrapper.dive().find({'data-test': 'subsectionContent'})

      expect(content.find(Guava).exists()).toBe(true)
    })

    it('only renders enabled tabs', () => {
      const wrapper = setup()
      const nav = wrapper.dive().find({'data-test': 'subsectionNav'})

      const tabs = nav.find(SubSectionsTab)

      tabs.forEach(tab => {
        expect(tab.exists()).toBe(tab.props().section.enabled)
      })
    })
  })
})
