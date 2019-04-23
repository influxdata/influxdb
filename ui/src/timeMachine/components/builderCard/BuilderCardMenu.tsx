// Libraries
import React, {PureComponent} from 'react'

interface Props {
  testID: string
}

export default class BuilderCardMenu extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--menu',
  }

  public render() {
    const {testID, children} = this.props
    return (
      <div className="builder-card--menu" data-testid={testID}>
        {children}
      </div>
    )
  }
}
