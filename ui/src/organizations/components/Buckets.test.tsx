// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import BucketList from 'src/organizations/components/BucketList'

// Constants
import {buckets} from 'mocks/dummyData'
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {withRouterProps} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    ...withRouterProps,
    buckets,
    emptyState: <></>,
    onUpdateBucket: jest.fn(),
    onDeleteBucket: jest.fn(),
    onSetBucketInfo: jest.fn(),
    onSetDataLoadersType: jest.fn(),
    dataLoaderType: DataLoaderType.Streaming,
    ...override,
  }

  const wrapper = renderWithRedux(<BucketList {...props} />)

  return {wrapper}
}

describe('BucketList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper).toMatchSnapshot()
    })
  })
})
