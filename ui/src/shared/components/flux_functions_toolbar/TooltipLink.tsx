import React, {PureComponent} from 'react'

interface Props {
  link: string
}

class TooltipLink extends PureComponent<Props> {
  public render() {
    const {link} = this.props

    return (
      <p>
        Still have questions? Check out the{' '}
        <a target="_blank" href={link}>
          Flux Docs
        </a>
        .
      </p>
    )
  }
}

export default TooltipLink
