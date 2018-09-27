import React from 'react'
import {mount} from 'enzyme'

import {Alignment} from 'src/clockface'
import IndexList from 'src/shared/components/index_views/IndexList'

describe('IndexList', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const emptyState = <div>Empty</div>

    const columns = [
      {
        key: 'test--fruit',
        size: 500,
        title: 'Fruit',
        align: Alignment.Left,
        showOnHover: false,
      },
      {
        key: 'test--calories',
        size: 500,
        title: 'Calories',
        align: Alignment.Left,
        showOnHover: false,
      },
    ]

    const rows = [
      {
        disabled: false,
        columns: [
          {
            key: 'test--fruit',
            contents: 'Apple',
          },
          {
            key: 'test--calories',
            contents: '500',
          },
        ],
      },
      {
        disabled: false,
        columns: [
          {
            key: 'test--fruit',
            contents: 'Pear',
          },
          {
            key: 'test--calories',
            contents: '1000',
          },
        ],
      },
      {
        disabled: false,
        columns: [
          {
            key: 'test--fruit',
            contents: 'Banana',
          },
          {
            key: 'test--calories',
            contents: '200',
          },
        ],
      },
    ]

    const props = {
      columns,
      rows,
      emptyState,
      ...override,
    }

    return mount(<IndexList {...props} />)
  }

  it('mounts without exploding', () => {
    wrapper = wrapperSetup()
    expect(wrapper).toHaveLength(1)
  })

  it('matches snapshot with minimal props', () => {
    wrapper = wrapperSetup()
    expect(wrapper).toMatchSnapshot()
  })

  it('renders empty state when 0 rows exist', () => {
    wrapper = wrapperSetup({rows: []})

    const emptyDiv = wrapper
      .find('div')
      .filterWhere(div => div.prop('data-test'))

    expect(emptyDiv.prop('data-test')).toBe('empty-state')
  })

  it('matches snapshot when 0 rows exist', () => {
    wrapper = wrapperSetup({rows: []})
    expect(wrapper).toMatchSnapshot()
  })
})
