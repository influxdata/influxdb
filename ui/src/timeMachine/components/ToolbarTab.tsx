// Libraries
import React, {PureComponent} from 'react'

interface Props {
  onSetActive: () => void
  name: string
  active: boolean
  testID: string
}

export default class ToolbarTab extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'toolbar-tab',
  }

  public render() {
    const {active, onSetActive, name, testID} = this.props
    return (
      <div
        className={`toolbar-tab ${active ? 'active' : ''}`}
        onClick={onSetActive}
        title={name}
        data-testid={testID}
      >
        {name}
      </div>
    )
  }
}
