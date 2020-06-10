import React from 'react'

import {render} from '@testing-library/react'

describe('the MarkdownRenderer wrapper around ReactMarkdown', () => {
  describe('image rendering behavior', () => {
    const text = '\n![](https://i.imgur.com/k3wIaNU.gif)\n'

    describe('in cloud contexts', () => {
      it('renders the image markdown literal, rather than rendering the image', () => {
        jest.mock('src/shared/constants', () => ({CLOUD: true}))
        const {
          MarkdownRenderer,
        } = require('src/shared/components/views/MarkdownRenderer.tsx')

        const {getByText} = render(<MarkdownRenderer text={text} />)
        expect(getByText('![](https://i.imgur.com/k3wIaNU.gif)'))
      })
    })

    describe('in oss contexts', () => {
      it('renders the image', () => {
        jest.mock('src/shared/constants', () => ({CLOUD: false}))
        const {
          MarkdownRenderer,
        } = require('src/shared/components/views/MarkdownRenderer.tsx')

        const {container} = render(<MarkdownRenderer text={text} />)
        expect(
          container.querySelector('img[src*="https://i.imgur.com/k3wIaNU.gif"]')
        )
      })
    })
  })
})
