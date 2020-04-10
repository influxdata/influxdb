// Libraries
import React, {FC} from 'react'
import ReactMarkdown, {Renderer, Renderers} from 'react-markdown'

import {CLOUD} from 'src/shared/constants/index'

interface Props {
  className?: string
  cloudRenderers?: Renderers
  text: string
}

// we don't want to render an image, but Renderer is generic and expects
// React element wrapping an image to be returned, so we use any
// https://github.com/rexxars/react-markdown/blob/master/index.d.ts#L101
const imageRenderer: Renderer<HTMLImageElement> = (props: HTMLImageElement): any => {
  return `![](${props.src})`
}

export const MarkdownRenderer: FC<Props> = ({className = '', cloudRenderers = {}, text}) => {
  // don't parse images in cloud environments to prevent arbitrary script execution via images
  if (CLOUD) {
    const renderers = {image: imageRenderer, imageReference: imageRenderer}
    return (
      <ReactMarkdown
        source={text}
        className={className}
        renderers={{...renderers, ...cloudRenderers}}
      />
    )
  }

  // load images locally to your heart's content. caveat emptor
  return (
    <ReactMarkdown
      source={text}
      className={className}
    />
  )
}
