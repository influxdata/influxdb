import React from 'react'

import {shallow} from 'enzyme'

import TagList from 'src/shared/components/TagList'
import TagListItem from 'src/shared/components/TagListItem'

import {query, source} from 'mocks/dummy'

const setup = (override = {}) => {
  const props = {
    onChooseTag: () => {},
    onGroupByTag: () => {},
    query,
    querySource: source,
    isQuerySupportedByExplorer: true,
    ...override,
  }

  TagList.prototype.getTags = jest.fn(() => Promise.resolve)
  const wrapper = shallow(<TagList {...props} />, {context: {source}})
  const instance = wrapper.instance() as TagList

  return {
    instance,
    props,
    wrapper,
  }
}

describe('Shared.Components.TagList', () => {
  describe('lifecycle methods', () => {
    describe('componentDidMount', () => {
      it('gets the tags', () => {
        const getTags = jest.fn()
        const {instance} = setup()
        instance.getTags = getTags
        instance.componentDidMount()

        expect(getTags).toHaveBeenCalledTimes(1)
      })

      describe('if there is no database', () => {
        it('does not get the tags', () => {
          const getTags = jest.fn()
          const {instance} = setup({query: {...query, database: ''}})
          instance.getTags = getTags
          instance.componentDidMount()

          expect(getTags).not.toHaveBeenCalled()
        })
      })

      describe('if there is no measurement', () => {
        it('does not get the tags', () => {
          const getTags = jest.fn()
          const {instance} = setup({query: {...query, measurement: ''}})
          instance.getTags = getTags
          instance.componentDidMount()

          expect(getTags).not.toHaveBeenCalled()
        })
      })

      describe('if there is no retention policy', () => {
        it('does not get the tags', () => {
          const getTags = jest.fn()
          const {instance} = setup({query: {...query, retentionPolicy: ''}})
          instance.getTags = getTags
          instance.componentDidMount()

          expect(getTags).not.toHaveBeenCalled()
        })
      })
    })

    describe('componentDidUpdate', () => {
      describe('if the db, rp, measurement, and source change and exist', () => {
        it('gets the tags', () => {
          const getTags = jest.fn()
          const updates = {
            database: 'newDb',
            measurement: 'newMeasurement',
            retentionPolicy: 'newRp',
          }

          const prevQuery = {...query, ...updates}
          const prevSource = {...source, id: 'prevID'}

          const {instance} = setup()
          instance.getTags = getTags
          instance.componentDidUpdate({
            query: prevQuery,
            querySource: prevSource,
          })

          expect(getTags).toHaveBeenCalledTimes(1)
        })

        describe('if there is no database', () => {
          it('does not get the tags', () => {
            const getTags = jest.fn()
            const {instance} = setup({query: {...query, database: ''}})
            instance.getTags = getTags
            instance.componentDidMount()

            expect(getTags).not.toHaveBeenCalled()
          })
        })

        describe('if there is no retentionPolicy', () => {
          it('does not get the tags', () => {
            const getTags = jest.fn()
            const {instance} = setup({query: {...query, retentionPolicy: ''}})
            instance.getTags = getTags
            instance.componentDidMount()

            expect(getTags).not.toHaveBeenCalled()
          })
        })

        describe('if there is no measurement', () => {
          it('does not get the tags', () => {
            const getTags = jest.fn()
            const {instance} = setup({query: {...query, measurement: ''}})
            instance.getTags = getTags
            instance.componentDidMount()

            expect(getTags).not.toHaveBeenCalled()
          })
        })
      })
    })
  })

  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    describe('when there are no tags', () => {
      it('does not render the <TagListItem/>', () => {
        const {wrapper} = setup()
        const tagListItems = wrapper.find(TagListItem)
        expect(tagListItems.length).toBe(0)
      })
    })

    describe('when there are tags', () => {
      it('does renders the <TagListItem/>', () => {
        const {wrapper} = setup()
        const values = ['tv1', 'tv2']
        const tags = {
          tk1: values,
          tk2: values,
        }

        wrapper.setState({tags})
        const tagListItems = wrapper.find(TagListItem)
        const first = tagListItems.first()
        const last = tagListItems.last()

        expect(tagListItems.length).toBe(2)
        expect(first.props().tagKey).toBe('tk1')
        expect(first.props().tagValues).toBe(values)
        expect(last.props().tagKey).toBe('tk2')
        expect(last.props().tagValues).toBe(values)
      })
    })
  })
})
