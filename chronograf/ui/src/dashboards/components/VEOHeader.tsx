// Libraries
import React, {PureComponent} from 'react'

// Components
import VEOHeaderName from 'src/dashboards/components/VEOHeaderName'
import {
  Radio,
  ButtonShape,
  Button,
  ComponentColor,
  ComponentSize,
  IconFont,
} from 'src/clockface'
import {Page} from 'src/page_layout'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface Props {
  viewName: string
  onSetViewName: (viewName: string) => void
  activeTab: TimeMachineTab
  onSetActiveTab: (activeTab: TimeMachineTab) => void
  onCancel: () => void
  onSave: () => void
}

class VEOHeader extends PureComponent<Props> {
  public render() {
    const {
      viewName,
      onSetViewName,
      activeTab,
      onSetActiveTab,
      onCancel,
      onSave,
    } = this.props

    return (
      <div className="veo-header">
        <Page.Header>
          <Page.Header.Left>
            <VEOHeaderName name={viewName} onRename={onSetViewName} />
          </Page.Header.Left>
          <Page.Header.Center>
            <Radio shape={ButtonShape.StretchToFit}>
              <Radio.Button
                id="deceo-tab-queries"
                titleText="Queries"
                value={TimeMachineTab.Queries}
                active={activeTab === TimeMachineTab.Queries}
                onClick={onSetActiveTab}
              >
                Queries
              </Radio.Button>
              <Radio.Button
                id="deceo-tab-vis"
                titleText="Visualization"
                value={TimeMachineTab.Visualization}
                active={activeTab === TimeMachineTab.Visualization}
                onClick={onSetActiveTab}
              >
                Visualization
              </Radio.Button>
            </Radio>
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
