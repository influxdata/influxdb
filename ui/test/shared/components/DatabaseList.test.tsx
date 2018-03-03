import React from 'react'
import DatabaseList from 'src/shared/components/DatabaseList'
import {shallow} from 'enzyme'

import {query, source} from 'test/resources'

const setup = (override = {}) => {
  const props = {
    query,
    source,
    querySource: source,
    onChooseNamespace: () => {},
    ...override,
  }

  DatabaseList.prototype.getDbRp = jest.fn(() => Promise.resolve())

  const dbList = shallow(<DatabaseList {...props} />, {
    context: {source},
  })

  return {
    dbList,
    props,
  }
}

describe('Shared.Components.DatabaseList', () => {
  describe('rendering', () => {
    it('can display the <DatabaseList/>', () => {
      const {dbList} = setup()

      expect(dbList.exists()).toBe(true)
    })
  })
})
