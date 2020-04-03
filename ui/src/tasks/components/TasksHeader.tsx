// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  InputLabel,
  SlideToggle,
  ComponentSize,
  ComponentStatus,
  Page,
} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'

interface Props {
  onCreateTask: () => void
  setShowInactive: () => void
  showInactive: boolean
  onImportTask: () => void
  limitStatus: LimitStatus
  onImportFromTemplate: () => void
}

export default class TasksHeader extends PureComponent<Props> {
  public render() {
    const {
      onCreateTask,
      setShowInactive,
      showInactive,
      onImportTask,
      onImportFromTemplate,
    } = this.props

    return (
      <>
        <Page.Header fullWidth={false}>
          <Page.Title title="Tasks" />
          <CloudUpgradeButton />
        </Page.Header>
        <Page.ControlBar fullWidth={false}>
          <Page.ControlBarLeft>
            <InputLabel>Show Inactive</InputLabel>
            <SlideToggle
              active={showInactive}
              size={ComponentSize.ExtraSmall}
              onChange={setShowInactive}
            />
          </Page.ControlBarLeft>
          <Page.ControlBarRight>
            <AddResourceDropdown
              canImportFromTemplate
              onSelectNew={onCreateTask}
              onSelectImport={onImportTask}
              onSelectTemplate={onImportFromTemplate}
              resourceName="Task"
              status={this.addResourceStatus}
            />
          </Page.ControlBarRight>
        </Page.ControlBar>
      </>
    )
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }
}
