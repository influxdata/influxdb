// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {Scraper} from 'src/onboarding/components/configureStep/Scraper'

const setup = (override = {}) => {
  const props = {
    bucket: '',
    dropdownBuckets: [<></>],
    onChooseInterval: jest.fn(),
    onDropdownHandle: jest.fn(),
    onAddRow: jest.fn(),
    onRemoveRow: jest.fn(),
    onUpdateRow: jest.fn(),
    tags: [{name: '', text: ''}],
    ...override,
  }
  const wrapper = shallow(<Scraper {...props} />)

  return {wrapper}
}

describe('Scraping', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })
})
