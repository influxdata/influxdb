import React, {SFC} from 'react'

interface Props {
  description: string
}

const TooltipDescription: SFC<Props> = ({description}) => (
  <article className="flux-functions-toolbar--description">
    <div className="flux-functions-toolbar--heading">Description</div>
    <span>{description}</span>
  </article>
)

export default TooltipDescription
