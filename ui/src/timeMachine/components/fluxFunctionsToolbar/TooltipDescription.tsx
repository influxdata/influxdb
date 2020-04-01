import React, {SFC} from 'react'

interface Props {
  description: string
}

const TooltipDescription: SFC<Props> = ({description}) => (
  <article className="flux-functions-toolbar--description">
    <div className="flux-function-docs--heading">Description</div>
    <span>{description}</span>
  </article>
)

export default TooltipDescription
