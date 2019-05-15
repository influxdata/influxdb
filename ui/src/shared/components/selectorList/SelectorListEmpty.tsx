// Libraries
import React, {PureComponent} from 'react'

interface Props {
  testID: string
}

export default class SelectorListEmpty extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'selector-list--empty',
  }

  public render() {
    const {testID, children} = this.props

    return (
      <div
        className="selector-list--body selector-list--empty"
        data-testid={testID}
      >
        {children}
      </div>
    )
  }
}
