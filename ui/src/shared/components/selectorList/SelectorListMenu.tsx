// Libraries
import React, {PureComponent} from 'react'

interface Props {
  testID: string
}

export default class SelectorListMenu extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'selector-list--menu',
  }

  public render() {
    const {testID, children} = this.props
    return (
      <div className="selector-list--menu" data-testid={testID}>
        {children}
      </div>
    )
  }
}
