import React, {SFC} from 'react'

interface Props {
  example: string
}

const TooltipExample: SFC<Props> = ({example}) => (
  <article>
    <div className="flux-function-docs--heading">Example</div>
    <div className="flux-function-docs--snippet">{example}</div>
  </article>
)

export default TooltipExample
