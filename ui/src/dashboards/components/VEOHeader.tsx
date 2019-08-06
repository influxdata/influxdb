// Libraries
import React, {PureComponent, MouseEvent} from 'react'

// Components
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  SquareButton,
  ComponentColor,
  ComponentSize,
  IconFont,
} from '@influxdata/clockface'
import {Page} from 'src/pageLayout'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import AlertingButton from 'src/timeMachine/components/AlertingButton'

// Constants
import {
  DEFAULT_CELL_NAME,
  CELL_NAME_MAX_LENGTH,
} from 'src/dashboards/constants/index'

interface Props {
  name: string
  onSetName: (name: string) => void
  onCancel: () => void
  onSave: () => void
}

const saveButtonClass = 'veo-header--save-cell-button'

class VEOHeader extends PureComponent<Props> {
  public render() {
    const {name, onSetName, onCancel, onSave} = this.props
    return (
      <div className="veo-header">
        <Page.Header fullWidth={true}>
          <Page.Header.Left>
            <RenamablePageTitle
              name={name}
              onRename={onSetName}
              placeholder={DEFAULT_CELL_NAME}
              maxLength={CELL_NAME_MAX_LENGTH}
              onClickOutside={this.handleClickOutsideTitle}
            />
          </Page.Header.Left>
          <Page.Header.Right>
            <AlertingButton />
            <ViewTypeDropdown />
            <VisOptionsButton />
            <SquareButton
              icon={IconFont.Remove}
              onClick={onCancel}
              size={ComponentSize.Small}
            />
            <SquareButton
              className={saveButtonClass}
              icon={IconFont.Checkmark}
              color={ComponentColor.Success}
              size={ComponentSize.Small}
              onClick={onSave}
              testID="save-cell--button"
            />
          </Page.Header.Right>
        </Page.Header>
      </div>
    )
  }

  private handleClickOutsideTitle = (e: MouseEvent<HTMLElement>) => {
    const {onSave} = this.props
    const target = e.target as HTMLButtonElement

    if (!target.className.includes(saveButtonClass)) {
      return
    }

    onSave()
  }
}

export default VEOHeader
