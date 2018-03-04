import React from 'react'
import MeasurementList from 'src/shared/components/MeasurementList'
import {shallow} from 'enzyme'
import {query, source} from 'test/resources'

const setup = (override = {}) => {
  const props = {
    query,
    querySource: source,
    onChooseTag: () => {},
    onGroupByTag: () => {},
    onToggleTagAcceptance: () => {},
    onChooseMeasurement: () => {},
    ...override,
  }

  MeasurementList.prototype._getMeasurements = jest.fn(() => Promise.resolve())

  const wrapper = shallow(<MeasurementList {...props} />, {
    context: {source},
  })

  const instance = wrapper.instance() as MeasurementList

  return {
    props,
    wrapper,
    instance,
  }
}

describe('Shared.Components.MeasurementList', () => {
  describe('rendering', () => {
    it('renders to the page', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('lifecycle methods', () => {
    describe('componentDidMount', () => {
      it('does not fire getMeasurements if there is no database', () => {
        const _getMeasurements = jest.fn()
        const {instance} = setup({query: {...query, database: ''}})
        instance._getMeasurements = _getMeasurements
        instance.componentDidMount()
        expect(_getMeasurements).not.toHaveBeenCalled()
      })

      it('does fire _getMeasurements if there is a database', () => {
        const _getMeasurements = jest.fn()
        const {instance} = setup()
        instance._getMeasurements = _getMeasurements
        instance.componentDidMount()
        expect(_getMeasurements).toHaveBeenCalled()
      })
    })

    describe('componentDidUpdate', () => {
      it('does not fire getMeasurements if there is no database', () => {
        const _getMeasurements = jest.fn()
        const {instance, props} = setup({query: {...query, database: ''}})

        instance._getMeasurements = _getMeasurements
        instance.componentDidUpdate(props)

        expect(_getMeasurements).not.toHaveBeenCalled()
      })

      it('does not fire _getMeasurements if the database does not change and the sources are equal', () => {
        const _getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance._getMeasurements = _getMeasurements
        instance.componentDidUpdate(props)

        expect(_getMeasurements).not.toHaveBeenCalled()
      })

      it('it fires _getMeasurements if there is a change in database', () => {
        const _getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance._getMeasurements = _getMeasurements
        instance.componentDidUpdate({
          ...props,
          query: {...query, database: 'diffDb'},
        })

        expect(_getMeasurements).toHaveBeenCalled()
      })

      it('it calls _getMeasurements if there is a change in source', () => {
        const _getMeasurements = jest.fn()
        const {instance, props} = setup()

        instance._getMeasurements = _getMeasurements
        instance.componentDidUpdate({
          ...props,
          querySource: {...source, id: 'newSource'},
        })

        expect(_getMeasurements).toHaveBeenCalled()
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
