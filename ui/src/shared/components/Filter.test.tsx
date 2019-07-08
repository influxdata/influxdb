import React, {Component} from 'react'
import {render} from 'react-testing-library'

import FilterList from 'src/shared/components/Filter'

function setup<T>(override?) {
  const props = {
    list: [],
    searchTerm: '',
    searchKeys: [],
    sortByKey: '',
    ...override,
  }

  return render(
    <FilterList<T> {...props}>
      {filtered =>
        filtered.map((item, index) => <TestListItem key={index} {...item} />)
      }
    </FilterList>
  )
}

describe('FilterList', () => {
  it('renders', () => {
    const list = [{name: 'foo'}, {name: 'bar'}]
    const {getAllByTestId} = setup<{name: string}>({list})

    const expected = getAllByTestId('list-item')
    expect(expected.length).toEqual(list.length)
  })

  it('renders a sorted list', () => {
    const itemOne = {name: 'foo'}
    const itemTwo = {name: 'bar'}
    const list = [itemOne, itemTwo]
    const {getAllByTestId} = setup<{name: string}>({list, sortByKey: 'name'})

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(list.length)
    expect(expected[0].textContent).toEqual(itemTwo.name)
    expect(expected[1].textContent).toEqual(itemOne.name)
  })

  it('can ignore casing in sorting', () => {
    const itemOne = {name: 'BBB'}
    const itemTwo = {name: 'aaa'}
    const itemThree = {name: 'CCC'}

    const list = [itemOne, itemTwo, itemThree]
    const {getAllByTestId} = setup<{name: string}>({list, sortByKey: 'name'})

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(list.length)
    expect(expected[0].textContent).toEqual(itemTwo.name)
    expect(expected[1].textContent).toEqual(itemOne.name)
    expect(expected[2].textContent).toEqual(itemThree.name)
  })

  it('filters list of flat objects', () => {
    const list = [{name: 'foo'}, {name: 'bar'}]
    const searchTerm = 'fo'
    const searchKeys = ['name']
    const {getAllByTestId} = setup<{name: string}>({
      list,
      searchTerm,
      searchKeys,
    })

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(1)
    expect(expected[0].textContent).toEqual('foo')
  })

  it('filters list of nested objects', () => {
    const itemOne = {
      id: '1',
      name: 'aPplication',
      labels: [{name: 'foo'}, {name: 'bar'}],
    }
    const itemTwo = {
      id: '2',
      name: 'ports',
      labels: [{name: 'aPple'}, {name: 'code'}],
    }
    const list = [
      itemOne,
      itemTwo,
      {
        id: '3',
        name: 'hipster',
        labels: [{name: 'bike'}, {name: 'glasses'}],
      },
    ]
    const searchTerm = 'ApP'
    const searchKeys = ['name', 'labels[].name']
    const {getAllByTestId} = setup<{
      labels: Array<{name: string}>
      id: string
      name: string
    }>({
      list,
      searchTerm,
      searchKeys,
    })

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(2)
    expect(expected[0].textContent).toEqual(itemOne.name)
    expect(expected[1].textContent).toEqual(itemTwo.name)
  })

  it('filters deeply nested inexact paths', () => {
    const itemOne = {
      id: '1',
      name: 'crackle',
      labels: [
        {
          name: 'snap',
          labels: [{name: 'super aPplication', labels: {name: 'pop'}}],
        },
      ],
    }
    const itemTwo = {
      id: '2',
      name: 'ports',
      labels: [
        {name: 'beep', labels: [{name: 'boop', labels: {name: 'RRRRRaPps'}}]},
      ],
    }

    const itemThree = {
      id: '3',
      name: 'rando',
      labels: [
        {name: 'TEST_APP', labels: [{name: 'a', labels: {name: 'match'}}]},
      ],
    }

    const list = [itemOne, itemTwo, itemThree]

    const searchTerm = 'ApP'
    const searchKeys = [
      'labels[].name',
      'labels[].labels[].name',
      'labels[].labels[].labels[].name',
    ]
    const {getAllByTestId} = setup<{
      labels: Array<{
        name: string
        labels: Array<{
          name: string
          labels: Array<{name: string}>
        }>
      }>
      id: string
      name: string
    }>({
      list,
      searchTerm,
      searchKeys,
    })

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(3)
    expect(expected[0].textContent).toEqual(itemOne.name)
    expect(expected[1].textContent).toEqual(itemTwo.name)
    expect(expected[2].textContent).toEqual(itemThree.name)
  })

  it('can filter nested objects', () => {
    const itemOne = {
      id: '1',
      name: 'crackle',
      properties: {
        description: 'a',
      },
    }

    const itemTwo = {
      id: '2',
      name: 'ports',
      properties: {
        description: 'b',
      },
    }

    const itemThree = {
      id: '3',
      name: 'rando',
      properties: {
        description: 'z',
      },
    }

    const list = [itemOne, itemTwo, itemThree]

    const searchTerm = 'Z'
    const searchKeys = ['name', 'properties.description']
    const {getAllByTestId} = setup({
      list,
      searchTerm,
      searchKeys,
    })

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(1)
    expect(expected[0].textContent).toEqual(itemThree.name)
  })

  it('errors when searchKey value is an object', () => {
    const itemOne = {
      name: {
        first: 'Hip',
        last: 'Bot',
      },
    }

    const list = [itemOne]
    const searchTerm = 'bo'
    const searchKeys = ['name']
    const consoleSpy = jest.spyOn(global.console, 'error')
    consoleSpy.mockImplementation(() => {})

    expect(() => {
      setup<{
        name: {
          first: string
          last: string
        }
      }>({
        list,
        searchTerm,
        searchKeys,
      })
    }).toThrowError()

    consoleSpy.mockRestore()
  })

  it('errors when inexact searchKey value is an object', () => {
    const itemOne = {
      things: [
        {
          name: {
            first: 'Hip',
            last: 'Bot',
          },
        },
      ],
    }

    const list = [itemOne]
    const searchTerm = 'be'
    const searchKeys = ['things[].name']
    const consoleSpy = jest.spyOn(global.console, 'error')
    consoleSpy.mockImplementation(() => {})

    expect(() => {
      setup<{
        things: Array<{
          name: {
            first: string
            last: string
          }
        }>
      }>({
        list,
        searchTerm,
        searchKeys,
      })
    }).toThrowError()

    consoleSpy.mockRestore()
  })
})

class TestListItem extends Component<any> {
  public render() {
    return <div data-testid="list-item">{this.props.name}</div>
  }
}
