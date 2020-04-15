import React, {Component} from 'react'
import {renderWithRedux} from 'src/mockState'

import FilterList from 'src/shared/components/FilterList'

const resources = {resources: {labels: {byID: {}, allIDs: []}}}

function setup<T>(override?, stateOverride = resources) {
  const props = {
    list: [],
    searchTerm: '',
    searchKeys: [],
    sortByKey: '',
    ...override,
  }

  const Filter = FilterList<T>()

  return renderWithRedux(
    <Filter {...props}>
      {filtered =>
        filtered.map((item, index) => <TestListItem key={index} {...item} />)
      }
    </Filter>,
    s => ({...s, ...stateOverride})
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
    const l1 = {id: 'l1', name: 'foo'}
    const l2 = {id: 'l2', name: 'bar'}
    const l3 = {id: 'l3', name: 'aPple'}
    const l4 = {id: 'l4', name: 'code'}
    const l5 = {id: 'l5', name: 'bike'}
    const l6 = {id: 'l6', name: 'glasses'}

    const appState = {
      resources: {
        labels: {
          byID: {
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
          },
          allIDs: [l1.id, l2.id, l3.id, l4.id, l5.id, l6.id],
        },
      },
    }

    const itemOne = {
      id: '1',
      name: 'aPplication',
      labels: [l1.id, l2.id],
    }
    const itemTwo = {
      id: '2',
      name: 'ports',
      labels: [l3.id, l4.id],
    }
    const list = [
      itemOne,
      itemTwo,
      {
        id: '3',
        name: 'hipster',
        labels: [l5.id, l6.id],
      },
    ]
    const searchTerm = 'ApP'
    const searchKeys = ['name', 'labels[].name']
    const {getAllByTestId} = setup<{
      labels: Array<{name: string}>
      id: string
      name: string
    }>(
      {
        list,
        searchTerm,
        searchKeys,
      },
      appState
    )

    const expected = getAllByTestId('list-item')

    expect(expected.length).toEqual(2)
    expect(expected[0].textContent).toEqual(itemOne.name)
    expect(expected[1].textContent).toEqual(itemTwo.name)
  })

  it('filters deeply nested inexact paths', () => {
    const l1 = {id: 'l1', name: 'super aPplication', labels: [{name: 'pop'}]}
    const l2 = {id: 'l2', name: 'beep', labels: [{name: 'RRRRRaPps'}]}
    const l3 = {
      id: 'l3',
      name: 'TEST_APP',
      labels: [{name: 'a', labels: [{name: 'match'}]}],
    }
    const l4 = {
      id: 'l4',
      name: 'snap',
      labels: [l1],
    }

    const appState = {
      resources: {
        labels: {
          byID: {
            l1,
            l2,
            l3,
            l4,
          },
          allIDs: [l1.id, l2.id, l3.id, l4.id],
        },
      },
    }

    const itemOne = {
      id: '1',
      name: 'crackle',
      labels: [l4.id],
    }
    const itemTwo = {
      id: '2',
      name: 'ports',
      labels: [l2.id],
    }

    const itemThree = {
      id: '3',
      name: 'rando',
      labels: [l3.id],
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
    }>(
      {
        list,
        searchTerm,
        searchKeys,
      },
      appState
    )

    const actual = getAllByTestId('list-item')

    expect(actual.length).toEqual(3)
    expect(actual[0].textContent).toEqual(itemOne.name)
    expect(actual[1].textContent).toEqual(itemTwo.name)
    expect(actual[2].textContent).toEqual(itemThree.name)
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
