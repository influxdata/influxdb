import React from 'react'
import {mount} from 'enzyme'

import IndexList from 'src/clockface/components/index_views/IndexList'

describe('IndexList', () => {
  let wrapper

  const wrapperSetup = (empty: boolean) => {
    const emptyState = <div>Empty</div>

    const header = (
      <IndexList.Header key="index-header">
        <IndexList.HeaderCell columnName="Fruit" width="50%" />
        <IndexList.HeaderCell columnName="Calories" width="50%" />
      </IndexList.Header>
    )

    const body = (
      <IndexList.Body key="index-body" columnCount={2} emptyState={emptyState}>
        <IndexList.Row>
          <IndexList.Cell>Apple</IndexList.Cell>
          <IndexList.Cell>500</IndexList.Cell>
        </IndexList.Row>
        <IndexList.Row>
          <IndexList.Cell>Pear</IndexList.Cell>
          <IndexList.Cell>1000</IndexList.Cell>
        </IndexList.Row>
        <IndexList.Row>
          <IndexList.Cell>Banana</IndexList.Cell>
          <IndexList.Cell>100</IndexList.Cell>
        </IndexList.Row>
      </IndexList.Body>
    )

    const emptyBody = (
      <IndexList.Body
        key="index-body"
        columnCount={2}
        emptyState={emptyState}
      />
    )

    let children = [header, body]

    if (empty) {
      children = [header, emptyBody]
    }

    const props = {
      children,
    }

    return mount(<IndexList {...props} />)
  }

  it('mounts without exploding', () => {
    wrapper = wrapperSetup(false)
    expect(wrapper).toHaveLength(1)
  })

  it('matches snapshot with minimal props', () => {
    wrapper = wrapperSetup(false)
    expect(wrapper).toMatchSnapshot()
  })

  it('renders empty state when 0 rows exist', () => {
    wrapper = wrapperSetup(true)

    const emptyDiv = wrapper
      .find('div')
      .filterWhere(div => div.prop('data-test'))

    expect(emptyDiv.prop('data-test')).toBe('empty-state')
  })

  it('matches snapshot when 0 rows exist', () => {
    wrapper = wrapperSetup(true)
    expect(wrapper).toMatchSnapshot()
  })
})
