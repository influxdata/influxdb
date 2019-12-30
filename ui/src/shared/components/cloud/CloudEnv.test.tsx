// Libraries
import React from 'react'
import {mount} from 'enzyme'

describe('If in cloud', () => {
  const Childish = <div>moo</div>

  it('CloudOnly renders children', () => {
    jest.mock('src/shared/constants', () => ({CLOUD: true}))
    const CloudOnly = require('src/shared/components/cloud/CloudOnly.tsx')
      .default
    const wrapper = mount(<CloudOnly>{Childish}</CloudOnly>)

    expect(wrapper.children().exists()).toBe(true)
    expect(wrapper.children().matchesElement(Childish)).toBe(true)
  })

  it('CloudExclude does not render chilren', () => {
    jest.mock('src/shared/constants', () => ({CLOUD: true}))
    const CloudExclude = require('src/shared/components/cloud/CloudExclude.tsx')
      .default
    const wrapper = mount(<CloudExclude>{Childish}</CloudExclude>)

    expect(wrapper.children().exists()).toBe(false)
  })
})
