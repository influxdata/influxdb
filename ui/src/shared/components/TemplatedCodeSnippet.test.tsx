
// Libraries
import React from 'react'
import {render} from 'react-testing-library'

describe('TeplatedCodeSnippet', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('should allow normal strings', () => {
    let props:any = {};
    jest.mock(
      'src/shared/components/CodeSnippet',
      () => ({ children, ...rest }) => {
        props = rest
        props.testing = 1

        return false
      }
    );

    const template = 'this is only a test'
    const Component = require('src/shared/components/TemplatedCodeSnippet').default
    const wrapper = render(<Component template={ template } />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(template)
    expect(props.testing).toEqual(1)
  })

  it('should show undefined', () => {
    let props:any = {};
    jest.mock(
      'src/shared/components/CodeSnippet',
      () => ({ children, ...rest }) => {
        props = rest;
        props.testing = 2

        return false
      }
    );

    const template = 'this is only a <%= word %>'
    const response = 'this is only a undefined'
    const Component = require('src/shared/components/TemplatedCodeSnippet').default
    const wrapper = render(<Component template={ template } />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
    expect(props.testing).toEqual(2)
  })

  it('should respect defaults', () => {
    let props:any = {};
    jest.mock(
      'src/shared/components/CodeSnippet',
      () => ({ children, ...rest }) => {
        props = rest;

        return false
      }
    );

    const template = 'this is only a <%= word %>'
    const response = 'this is only a sentance'
    const defaults = {
      word: 'sentance'
    }
    const Component = require('src/shared/components/TemplatedCodeSnippet').default
    const wrapper = render(<Component template={ template } defaults={ defaults } />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
  })

  it('should overwrite defaults with values', () => {
    let props:any = {};
    jest.mock(
      'src/shared/components/CodeSnippet',
      () => ({ children, ...rest }) => {
        props = rest;

        return false
      }
    );

    const template = 'this is only a <%= word %>'
    const response = 'this is only a test'
    const defaults = {
      word: 'sentance'
    }
    const values = {
      word: 'test'
    }
    const Component = require('src/shared/components/TemplatedCodeSnippet').default
    const wrapper = render(<Component template={ template } defaults={ defaults } values={ values } />)

    expect(props.hasOwnProperty('copyText')).toBe(true)
    expect(props.copyText).toEqual(response)
  })
})
