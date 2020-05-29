// Libraries
import React from 'react'
import {mount} from 'enzyme'

describe('If not Cloud', () => {
  const Childish = <div>moo</div>
  it('CloudOnly does not render children', () => {
    jest.mock('src/shared/constants', () => ({CLOUD: false}))
    const CloudOnly = require('src/shared/components/cloud/CloudOnly.tsx')
      .default
    const wrapper = mount(<CloudOnly>{Childish}</CloudOnly>)

    expect(wrapper.children().exists()).toBe(false)
  })

  it('CloudExclude renders chilren', () => {
    jest.mock('src/shared/constants', () => ({CLOUD: false}))
    const CloudExclude = require('src/shared/components/cloud/CloudExclude.tsx')
      .default
    const wrapper = mount(<CloudExclude>{Childish}</CloudExclude>)

    expect(wrapper.children().exists()).toBe(true)
    expect(wrapper.children().matchesElement(Childish)).toBe(true)
  })
})
