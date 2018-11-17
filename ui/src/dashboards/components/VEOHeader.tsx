// Libraries
import React, {PureComponent} from 'react'

// Components
import VEOHeaderName from 'src/dashboards/components/VEOHeaderName'
import TimeMachineTabs from 'src/shared/components/TimeMachineTabs'
import {
  ButtonShape,
  Button,
  ComponentColor,
  ComponentSize,
  IconFont,
} from 'src/clockface'
import {Page} from 'src/pageLayout'

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
        <Page.Header>
          <Page.Header.Left>
            <VEOHeaderName name={name} onRename={onSetName} />
          </Page.Header.Left>
          <Page.Header.Center>
            <TimeMachineTabs />
          </Page.Header.Center>
          <Page.Header.Right>
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
