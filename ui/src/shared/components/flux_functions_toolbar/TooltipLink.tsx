import React, {SFC} from 'react'

interface Props {
  link: string
}

const TooltipLink: SFC<Props> = ({link}) => (
  <p>
    Still have questions? Check out the{' '}
    <a target="_blank" href={link}>
      Flux Docs
    </a>
    .
  </p>
)

export default TooltipLink
