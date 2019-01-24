// Libraries
import React, {PureComponent} from 'react'

// Components
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  ButtonShape,
  Button,
  ComponentColor,
  ComponentSize,
  IconFont,
} from 'src/clockface'
import {Page} from 'src/pageLayout'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'

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
            />
          </Page.Header.Left>
          <Page.Header.Right>
            <ViewTypeDropdown />
            <VisOptionsButton />
            <Button
              icon={IconFont.Remove}
              shape={ButtonShape.Square}
              onClick={onCancel}
              size={ComponentSize.Small}
            />
            <Button
              icon={IconFont.Checkmark}
              shape={ButtonShape.Square}
              color={ComponentColor.Success}
              size={ComponentSize.Small}
              onClick={onSave}
            />
          </Page.Header.Right>
        </Page.Header>
      </div>
    )
  }
}

export default VEOHeader
