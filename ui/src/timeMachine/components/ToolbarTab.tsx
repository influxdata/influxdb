// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/timeMachine/components/ToolbarTab.scss'

interface PassedProps {
  onSetActive: () => void
  name: string
  active: boolean
}

interface DefaultProps {
  testID?: string
}

type Props = PassedProps & DefaultProps

export default class ToolbarTab extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
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
