// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import _ from 'lodash'

// Components
import {
  IconFont,
  ComponentColor,
  ComponentSize,
  Dropdown,
  ComponentStatus,
} from '@influxdata/clockface'

// Actions
import {showOverlay, dismissOverlay} from 'src/overlays/actions/overlays'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'

// Constants
import {CLOUD} from 'src/shared/constants'

interface OwnProps {
  onSelectNew: () => void
  resourceName: string
  limitStatus?: LimitStatus
}

interface DefaultProps {
  status: ComponentStatus
  titleText: string
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = OwnProps & DefaultProps & ReduxProps

class AddResourceButton extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    status: ComponentStatus.Default,
    titleText: null,
  }

  public render() {
    const {titleText, status} = this.props
    return (
      <Dropdown
        style={{width: '190px'}}
        testID="add-resource-dropdown"
        button={(active, onClick) => (
          <Dropdown.Button
            testID="add-resource-dropdown--button"
            active={active}
            onClick={onClick}
            color={ComponentColor.Primary}
            size={ComponentSize.Small}
            icon={IconFont.Plus}
            status={status}
          >
            {titleText || `Create ${this.props.resourceName}`}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu
            onCollapse={onCollapse}
            testID="add-resource-dropdown--menu"
          >
            {this.optionItems}
          </Dropdown.Menu>
        )}
      >
        {this.optionItems}
      </Dropdown>
    )
  }

  private get optionItems(): JSX.Element[] {
    const importOption = this.importOption
    const newOption = this.newOption

    const items = [
      <Dropdown.Item
        id={importOption}
        key={importOption}
        onClick={this.handleSelect}
        value={importOption}
        testID="add-resource-dropdown--import"
      >
        {importOption}
      </Dropdown.Item>,
    ]

    return items
  }

  private get newOption(): string {
    return `New ${this.props.resourceName}`
  }

  private get importOption(): string {
    return `Import ${this.props.resourceName}`
  }

  private handleLimit = (): void => {
    const {resourceName, onShowOverlay, onDismissOverlay} = this.props
    onShowOverlay('asset-limit', {asset: `${resourceName}s`}, onDismissOverlay)
  }

  private handleSelect = (selection: string): void => {
    const {onSelectNew, limitStatus = LimitStatus.OK} = this.props

    if (CLOUD && limitStatus === LimitStatus.EXCEEDED) {
      this.handleLimit()
      return
    }

    if (selection === this.newOption) {
      onSelectNew()
    }
  }
}

const mdtp = {
  onShowOverlay: showOverlay,
  onDismissOverlay: dismissOverlay,
}

const connector = connect(null, mdtp)

export default connector(AddResourceButton)
