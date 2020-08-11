// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

interface Props {
  testID: string
  className?: string
}

export default class BuilderCardMenu extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--menu',
  }

  public render() {
    const {testID, children, className} = this.props
    const classname = classnames('builder-card--menu', {
      [`${className}`]: className,
    })

    return (
      <div className={classname} data-testid={testID}>
        {children}
      </div>
    )
  }
}
