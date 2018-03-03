import React from 'react'
import DatabaseList from 'src/shared/components/DatabaseList'
import {shallow} from 'enzyme'

const source = {
  links: {
    self: '/chronograf/v1/sources/16',
    kapacitors: '/chronograf/v1/sources/16/kapacitors',
    proxy: '/chronograf/v1/sources/16/proxy',
    queries: '/chronograf/v1/sources/16/queries',
    write: '/chronograf/v1/sources/16/write',
    permissions: '/chronograf/v1/sources/16/permissions',
    users: '/chronograf/v1/sources/16/users',
    databases: '/chronograf/v1/sources/16/dbs',
  },
}

const setup = async (override = {}) => {
  const props = {
    query: {},
    source: {},
    querySource: {},
    onChooseNamespace: () => {},
    ...override,
  }

  DatabaseList.prototype.getDbRp = jest.fn(() => Promise.resolve())

  const dbList = await shallow(<DatabaseList {...props} />, {context: {source})

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
