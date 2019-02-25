import React, {SFC} from 'react'

interface Props {
  example: string
}

const TooltipExample: SFC<Props> = ({example}) => (
  <article>
    <div className="flux-functions-toolbar--heading">Example</div>
    <div className="flux-functions-toolbar--snippet">{example}</div>
  </article>
)

export default TooltipExample
