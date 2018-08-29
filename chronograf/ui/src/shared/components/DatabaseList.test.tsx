import {shallow} from 'enzyme'
import React from 'react'
import DatabaseList from 'src/shared/components/DatabaseList'
import DatabaseListItem from 'src/shared/components/DatabaseListItem'

import {query, source} from 'mocks/dummy'

// mock data
const dbrp1 = {database: 'db1', retentionPolicy: 'rp1'}
const dbrp2 = {database: 'db2', retentionPolicy: 'rp2'}
const namespaces = [dbrp1, dbrp2]

const setup = (override = {}) => {
  const props = {
    onChooseNamespace: () => {},
    query,
    querySource: source,
    source,
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

    it('renders the correct number of <DatabaseListItems', () => {
      const {dbList} = setup()
      // replace with mock of getDbRp()?
      dbList.setState({namespaces})
      const list = dbList.find(DatabaseListItem)
      expect(list.length).toBe(2)
    })
  })

  describe('instance methods', () => {
    describe('handleChooseNamespace', () => {
      it('fires onChooseNamespace with a namspace arg', () => {
        const onChooseNamespace = jest.fn()
        const {dbList} = setup({onChooseNamespace})

        const instance = dbList.instance() as DatabaseList
        instance.handleChooseNamespace(dbrp1)()

        expect(onChooseNamespace).toHaveBeenCalledTimes(1)
        expect(onChooseNamespace).toHaveBeenCalledWith(dbrp1)
      })
    })

    describe('isActive', () => {
      describe('if the query does not match the db and rp', () => {
        it('returns false', () => {
          const {dbList} = setup()

          const instance = dbList.instance() as DatabaseList
          expect(instance.isActive(query, dbrp1)).toBe(false)
        })

        describe('if the query matches the db and rp', () => {
          it('returns true', () => {
            const {database, retentionPolicy} = dbrp1
            const matchingQuery = {
              ...query,
              database,
              retentionPolicy,
            }

            const {dbList} = setup()
            const instance = dbList.instance() as DatabaseList

            expect(instance.isActive(matchingQuery, dbrp1)).toBe(true)
          })
        })
      })
    })
  })
})
