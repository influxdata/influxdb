// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Constants
import {dashboardImportFailed} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {getDashboardsAsync} from 'src/dashboards/actions/v2'

// Types
import ImportOverlay from 'src/shared/components/ImportOverlay'
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions/v2'

interface OwnProps {
  onDismissOverlay: () => void
  isVisible: boolean
}
interface DispatchProps {
  notify: typeof notifyAction
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
  populateDashboards: typeof getDashboardsAsync
}

type Props = OwnProps & DispatchProps

class ImportDashboardOverlay extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {isVisible, onDismissOverlay} = this.props

    return (
      <ImportOverlay
        isVisible={isVisible}
        onDismissOverlay={onDismissOverlay}
        resourceName="Dashboard"
        onSubmit={this.handleUploadDashboard}
      />
    )
  }

  private handleUploadDashboard = async (
    uploadContent: string,
    orgID: string
  ): Promise<void> => {
    const {
      notify,
      createDashboardFromTemplate,
      onDismissOverlay,
      populateDashboards,
    } = this.props

    try {
      const template = JSON.parse(uploadContent)

      await createDashboardFromTemplate(template, orgID)
      await populateDashboards()

      onDismissOverlay()
    } catch (error) {
      notify(dashboardImportFailed(error))
    }
  }
}
const mdtp: DispatchProps = {
  notify: notifyAction,
  createDashboardFromTemplate: createDashboardFromTemplateAction,
  populateDashboards: getDashboardsAsync,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(ImportDashboardOverlay)
