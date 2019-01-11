import React from 'react'
import {shallow} from 'enzyme'

import {Page} from 'src/pageLayout'

describe('PageHeader', () => {
  let wrapper

  const wrapperSetup = (
    overrideProps = {},
    overrideLeft = (
      <Page.Header.Left>
        <Page.Title title="Yeehaw" />
      </Page.Header.Left>
    ),
    overrideCenter = (
      <Page.Header.Center>
        <p>Center!</p>
      </Page.Header.Center>
    ),
    overrideRight = <Page.Header.Right />
  ) => {
    const props = {
      fullWidth: false,
      inPresentationMode: false,
      ...overrideProps,
    }

    return shallow(
      <Page.Header {...props}>
        {overrideLeft}
        {overrideCenter}
        {overrideRight}
      </Page.Header>
    )
  }

  it('mounts without exploding', () => {
    wrapper = wrapperSetup()
    expect(wrapper).toHaveLength(1)
  })

  it('matches snapshot with minimal props', () => {
    wrapper = wrapperSetup()
    expect(wrapper).toMatchSnapshot()
  })

  it('matches snapshot in presentation mode', () => {
    wrapper = wrapperSetup({inPresentationMode: true})
    expect(wrapper).toMatchSnapshot()
  })

  it('should contain matching error text if incorrect child types are passed in', done => {
    try {
      wrapper = wrapperSetup({}, <p />, <p />, <p />)
    } catch (error) {
      expect(error.toString()).toEqual(
        'Error: <Page.Header> expected children of type <Page.Header.Left>, <Page.Header.Center>, or <Page.Header.Right>'
      )
      done()
    }
  })

  it('should contain matching error text if too many of one header type exist', done => {
    try {
      wrapper = wrapperSetup(
        {},
        <Page.Header.Right />,
        <Page.Header.Right />,
        <Page.Header.Right />
      )
    } catch (error) {
      expect(error.toString()).toEqual(
        'Error: <Page.Header> expects at most 1 of each child type: <Page.Header.Left>, <Page.Header.Center>, or <Page.Header.Right>'
      )
      done()
    }
  })
})
