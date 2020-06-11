// Libraries
import React from 'react'
import {render} from '@testing-library/react'

describe('TeplatedCodeSnippet', () => {
  beforeEach(() => {
    // NOTE: as long as you mock children like below, before importing your
    // component by using a require().default pattern, this will reset your
    // mocks between tests (alex)
    jest.resetModules()
  })

  it('should allow normal strings', () => {
    let props: any = {}
    jest.mock('src/shared/components/CodeSnippet', () => _props => {
      props = _props
      return false
    })

    const template = 'this is only a test'
    const Component = require('src/shared/components/TemplatedCodeSnippet')
      .default
    render(<Component template={template} />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(template)
  })

  it('should show undefined', () => {
    let props: any = {}
    jest.mock('src/shared/components/CodeSnippet', () => _props => {
      props = _props
      return false
    })

    const template = 'this is only a <%= word %>'
    const response = 'this is only a undefined'
    const Component = require('src/shared/components/TemplatedCodeSnippet')
      .default
    render(<Component template={template} />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
  })

  it('should respect defaults', () => {
    let props: any = {}
    jest.mock('src/shared/components/CodeSnippet', () => _props => {
      props = _props
      return false
    })

    const template = 'this is only a <%= word %>'
    const response = 'this is only a sentance'
    const defaults = {
      word: 'sentance',
    }
    const Component = require('src/shared/components/TemplatedCodeSnippet')
      .default
    render(<Component template={template} defaults={defaults} />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
  })

  it('should overwrite defaults with values', () => {
    let props: any = {}
    jest.mock('src/shared/components/CodeSnippet', () => _props => {
      props = _props

      return false
    })

    const template = 'this is only a <%= word %>'
    const response = 'this is only a test'
    const defaults = {
      word: 'sentance',
    }
    const values = {
      word: 'test',
    }
    const Component = require('src/shared/components/TemplatedCodeSnippet')
      .default
    render(
      <Component template={template} defaults={defaults} values={values} />
    )

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
  })
})
