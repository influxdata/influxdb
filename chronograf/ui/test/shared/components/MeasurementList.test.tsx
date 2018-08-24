import {shallow} from 'enzyme'
import React from 'react'
import MeasurementList from 'src/shared/components/MeasurementList'
import MeasurementListFilter from 'src/shared/components/MeasurementListFilter'
import MeasurementListItem from 'src/shared/components/MeasurementListItem'
import {query, source} from 'test/resources'

const setup = (override = {}) => {
  const props = {
    onChooseMeasurement: () => {},
    onChooseTag: () => {},
    onGroupByTag: () => {},
    onToggleTagAcceptance: () => {},
    query,
    querySource: source,
    source,
    isQuerySupportedByExplorer: true,
    ...override,
  }

  MeasurementList.prototype.getMeasurements = jest.fn(() => Promise.resolve())

  const wrapper = shallow(<MeasurementList {...props} />, {
    context: {source},
  })

  const instance = wrapper.instance() as MeasurementList

  return {
    instance,
    props,
    wrapper,
  }
}

describe('Shared.Components.MeasurementList', () => {
  describe('rendering', () => {
    it('renders to the page', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    describe('<MeasurementListItem/>', () => {
      it('renders <MeasurementListItem/>`s to the page', () => {
        const {wrapper} = setup()
        wrapper.setState({filtered: ['foo', 'bar']})
        const items = wrapper.find(MeasurementListItem)
        const first = items.first()
        const last = items.last()

        expect(items.length).toBe(2)
        expect(first.dive().text()).toContain('foo')
        expect(last.dive().text()).toContain('bar')
      })

      it('renders <MeasurementListFilter/> to the page', () => {
        const {wrapper} = setup()
        const filter = wrapper.find(MeasurementListFilter)

        expect(filter.exists()).toBe(true)
      })
    })
  })

  describe('user interactions', () => {
    it('can filter the measurement list', () => {
      const {wrapper} = setup()
      const measurements = ['foo', 'bar']
      const event = {target: {value: 'f'}, stopPropagation: () => {}}
      wrapper.setState({filtered: measurements, measurements})

      const filter = wrapper.find(MeasurementListFilter)
      const input = filter.dive().find('input')
      input.simulate('change', event)
      wrapper.update()

      const items = wrapper.find(MeasurementListItem)
      expect(items.length).toBe(1)
      expect(items.dive().text()).toBe('foo')
    })
  })

  describe('lifecycle methods', () => {
    describe('componentDidMount', () => {
      it('does not fire getMeasurements if there is no database', () => {
        const getMeasurements = jest.fn()
        const {instance} = setup({query: {...query, database: ''}})
        instance.getMeasurements = getMeasurements
        instance.componentDidMount()
        expect(getMeasurements).not.toHaveBeenCalled()
      })

      it('does fire getMeasurements if there is a database', () => {
        const getMeasurements = jest.fn()
        const {instance} = setup()
        instance.getMeasurements = getMeasurements
        instance.componentDidMount()
        expect(getMeasurements).toHaveBeenCalled()
      })
    })

    describe('componentDidUpdate', () => {
      it('does not fire getMeasurements if there is no database', () => {
        const getMeasurements = jest.fn()
        const {instance, props} = setup({query: {...query, database: ''}})

        instance.getMeasurements = getMeasurements
        instance.componentDidUpdate(props)

        expect(getMeasurements).not.toHaveBeenCalled()
      })

      it('does not fire getMeasurements if the database does not change and the sources are equal', () => {
        const getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance.getMeasurements = getMeasurements
        instance.componentDidUpdate(props)

        expect(getMeasurements).not.toHaveBeenCalled()
      })

      it('it fires getMeasurements if there is a change in database', () => {
        const getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance.getMeasurements = getMeasurements
        instance.componentDidUpdate({
          ...props,
          query: {...query, database: 'diffDb'},
        })

        expect(getMeasurements).toHaveBeenCalled()
      })

      it('it calls getMeasurements if there is a change in source', () => {
        const getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance.getMeasurements = getMeasurements
        instance.componentDidUpdate({
          ...props,
          querySource: {...source, id: 'newSource'},
        })

        expect(getMeasurements).toHaveBeenCalled()
      })
    })
  })

  describe('instance methods', () => {
    describe('handleFilterText', () => {
      it('sets the filterText state to the event targets value', () => {
        const {instance} = setup()
        const value = 'spectacs'
        const stopPropagation = () => {}
        const event = {target: {value}, stopPropagation}

        instance.handleFilterText(event)

        expect(instance.state.filterText).toBe(value)
      })
    })

    describe('handleEscape', () => {
      it('resets fiterText when escape is pressed', () => {
        const {instance} = setup()
        const key = 'Escape'
        const stopPropagation = () => {}
        const event = {key, stopPropagation}

        instance.setState({filterText: 'foo'})
        expect(instance.state.filterText).toBe('foo')

        instance.handleEscape(event)
        expect(instance.state.filterText).toBe('')
      })

      it('does not reset fiterText when escape is pressed', () => {
        const {instance} = setup()
        const key = 'Enter'
        const stopPropagation = () => {}
        const event = {key, stopPropagation}

        instance.setState({filterText: 'foo'})
        instance.handleEscape(event)
        expect(instance.state.filterText).toBe('foo')
      })
    })

    describe('handleAcceptReject', () => {
      it('it calls onToggleTagAcceptance', () => {
        const onToggleTagAcceptance = jest.fn()
        const {instance} = setup({onToggleTagAcceptance})

        instance.handleAcceptReject()
        expect(onToggleTagAcceptance).toHaveBeenCalledTimes(1)
      })
    })
  })
})
