import {parseAlerta} from 'shared/parsing/parseAlerta'

it('can parse an alerta tick script', () => {
  const tickScript = `stream
    |alert()
      .alerta()
        .resource('Hostname or service')
        .event('Something went wrong')
        .environment('Development')
        .group('Dev. Servers')
        .services('a b c')
  `

  let actualObj = parseAlerta(tickScript)

  const expectedObj = [
    {
      name: 'resource',
      args: ['Hostname or service'],
    },
    {
      name: 'event',
      args: ['Something went wrong'],
    },
    {
      name: 'environment',
      args: ['Development'],
    },
    {
      name: 'group',
      args: ['Dev. Servers'],
    },
    {
      name: 'services',
      args: ['a', 'b', 'c'],
    },
  ]

  // Test data structure
  expect(actualObj).to.deep.equal(expectedObj)

  // Test that data structure is the same if fed back in
  const expectedStr = `alerta().resource('Hostname or service').event('Something went wrong').environment('Development').group('Dev. Servers').services('a b c')`
  actualObj = parseAlerta(expectedStr)
  expect(actualObj).to.deep.equal(expectedObj)
})
