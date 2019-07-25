// Libraries
import React from 'react'
import {render} from 'react-testing-library'

// Components
import BucketsDropdown from 'src/dataLoaders/components/BucketsDropdown'
import {bucket} from 'mocks/dummyData'

// Mocks

const setup = (override = {}) => {
  const props = {
    selectedBucketID: '',
    buckets: [],
    onSelectBucket: jest.fn(),
    ...override,
  }

  return render(<BucketsDropdown {...props} />)
}

describe('DataLoading.Components.BucketsDropdown', () => {
  describe('when no buckets', () => {
    it('renders disabled dropdown', () => {
      const {getByTestId} = setup()

      const dropdown = getByTestId('bucket-dropdown')
      const dropdownButton = getByTestId('bucket-dropdown--button')

      expect(dropdown).toBeDefined()
      expect(dropdownButton.getAttribute('disabled')).toBe('')
    })
  })

  describe('if buckets', () => {
    it('renders dropdown', () => {
      const buckets = [bucket]
      const selectedBucketID = bucket.id
      const {getByTestId} = setup({buckets, selectedBucketID})

      const dropdown = getByTestId('bucket-dropdown')
      const dropdownButton = getByTestId('bucket-dropdown--button')

      expect(dropdown).toBeDefined()
      expect(dropdownButton.getAttribute('disabled')).toBe(null)
    })
  })
})
