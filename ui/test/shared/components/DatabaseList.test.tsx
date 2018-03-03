import React from 'react'
import DatabaseList from 'src/shared/components/DatabaseList'
import DatabaseListItem from 'src/shared/components/DatabaseListItem'
import {shallow} from 'enzyme'

import {query, source} from 'test/resources'

// mock data
const dbrp1 = {database: 'db1', retentionPolicy: 'rp1'}
const dbrp2 = {database: 'db2', retentionPolicy: 'rp2'}
const namespaces = [dbrp1, dbrp2]

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

        // TODO: find out why instance method is undefined
        dbList.instance().handleChooseNamespace(dbrp1)

        expect(onChooseNamespace).toHaveBeenCalledTimes(1)
        expect(onChooseNamespace).toHaveBeenCalledWith(dbrp1)
      })
    })

    describe('isActive', () => {
      describe('if the query does not match the db and rp', () => {
        it('returns false', () => {
          const {dbList} = setup()

          // TODO: find out why ts is not finding this
          // instance method is undefined
          const isActive = dbList.instance().isActive(query, dbrp1)
          expect(isActive).toBe(false)
        })

        describe('if the query matches the db and rp', () => {
          it('returns true', () => {
            const matchingQuery = {
              ...query,
              db: dbrp1.database,
              retentionPolicy: dbrp1.retentionPolicy,
            }

            const {dbList} = setup()

            // TODO: find out why instance method is undefined
            const isActive = dbList.instance().isActive(matchingQuery, dbrp1)
            expect(isActive).toBe(true)
          })
        })
      })
    })
  })
})
