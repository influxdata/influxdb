// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  SlideToggle,
  ComponentSize,
  ComponentStatus,
  Page,
} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import {displayOverlay} from 'src/overlays/components/OverlayLink'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'

interface OwnProps {
  onCreateTask: () => void
  setShowInactive: () => void
  showInactive: boolean
  limitStatus: LimitStatus
}

type Props = OwnProps & WithRouterProps

class TasksHeader extends PureComponent<Props> {
  public render() {
    const {
      onCreateTask,
      setShowInactive,
      showInactive,
      router,
      location,
    } = this.props

    const handleOpenImportOverlay = displayOverlay(
      location.pathname,
      router,
      'import-task'
    )
    const handleOpenCreateFromTemplateOverlay = displayOverlay(
      location.pathname,
      router,
      'create-task-from-template'
    )

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <PageTitleWithOrg title="Tasks" />
        </Page.Header.Left>
        <Page.Header.Right>
          <SlideToggle.Label text="Show Inactive" />
          <SlideToggle
            active={showInactive}
            size={ComponentSize.ExtraSmall}
            onChange={setShowInactive}
          />
          <AddResourceDropdown
            canImportFromTemplate={true}
            onSelectNew={onCreateTask}
            onSelectImport={handleOpenImportOverlay}
            onSelectTemplate={handleOpenCreateFromTemplateOverlay}
            resourceName="Task"
            status={this.addResourceStatus}
          />
        </Page.Header.Right>
      </Page.Header>
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

export default withRouter<OwnProps>(TasksHeader)
