import React, {Component} from 'react'

interface Props {
  className?: string
}
class LoadingDots extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    className: '',
  }

  public render() {
    const {className} = this.props

    return (
      <div className={`loading-dots ${className}`}>
        <div />
        <div />
        <div />
      </div>
    )
  }
}

export default LoadingDots
