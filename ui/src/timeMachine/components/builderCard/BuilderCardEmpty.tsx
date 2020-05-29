// Libraries
import React, {PureComponent} from 'react'

interface Props {
  testID: string
}

export default class BuilderCardEmpty extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'builder-card--empty',
  }

  public render() {
    const {testID, children} = this.props

    return (
      <div
        className="builder-card--body builder-card--empty"
        data-testid={testID}
      >
        {children}
      </div>
    )
  }
}
