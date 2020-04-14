import React from 'react'

jest.mock('honeybadger-js', () => {
  return {
    configure: jest.fn(),
    notify: jest.fn(),
  }
})

import {render} from 'react-testing-library'

describe('the NoteEditor markdown renderer', () => {
  describe('image rendering behavior', () => {
    const note = '\n![](https://i.imgur.com/k3wIaNU.gif)\n'

    describe('in cloud contexts', () => {
      it("renders a warning that we can't render images, rather than rendering the image", () => {
        jest.mock('src/shared/constants', () => ({CLOUD: true}))
        const NoteEditorPreview = require('src/dashboards/components/NoteEditorPreview.tsx')
          .default

        const {getByText} = render(
          <NoteEditorPreview note={note} scrollTop={0} onScroll={() => {}} />
        )
        expect(
          getByText("We don't support images in markdown for security purposes")
        )
      })
    })

    describe('in oss contexts', () => {
      it('renders the image', () => {
        jest.mock('src/shared/constants', () => ({CLOUD: false}))
        const NoteEditorPreview = require('src/dashboards/components/NoteEditorPreview.tsx')
          .default
        const {container} = render(
          <NoteEditorPreview note={note} scrollTop={0} onScroll={() => {}} />
        )
        expect(
          container.querySelector('img[src*="https://i.imgur.com/k3wIaNU.gif"]')
        )
      })
    })
  })
})
