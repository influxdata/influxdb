import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

import {getDeep} from 'src/utils/wrappers'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'
import {notifyDashboardImportFailed} from 'src/shared/copy/notifications'

import {Dashboard} from 'src/types'
import {DashboardFile} from 'src/types/dashboard'
import {Notification} from 'src/types/notifications'

interface Props {
  onDismissOverlay: () => void
  onImportDashboard: (dashboard: Dashboard) => void
  notify: (message: Notification) => void
}

interface DashboardFromFile {
  dashboard: Dashboard | null
  fileName: string
}

interface State {
  dashboardFromFile: DashboardFromFile
}

interface File extends Blob {
  lastModified: number
  lastModifiedDate: Date
  name: string
  size: number
  type: string
  webkitRelativePath: string
}

class ImportDashboardOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      dashboardFromFile: null,
    }
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={800}>
        <Heading title="Import Dashboard" onDismiss={onDismissOverlay} />
        <Body>
          <form onSubmit={this.handleImportDashboard}>
            <div className="form-group col-sm-6">
              <label htmlFor="dashboardUploader">Choose File to Import</label>
              <input
                id="dashboardUploader"
                type="file"
                onChange={this.handleChooseFile}
                accept=".json"
              />
            </div>
            <div className="form-group form-group-submit col-xs-12 text-center">
              <button className="btn btn btn-success">Import</button>
            </div>
          </form>
        </Body>
      </Container>
    )
  }

  private handleChooseFile = (e: ChangeEvent<HTMLInputElement>): void => {
    const file: File = getDeep(e, 'target.files[0]', null)
    const fileReader = new FileReader()
    fileReader.onloadend = () => {
      const result: DashboardFile = JSON.parse(fileReader.result)
      const dashboard = getDeep<Dashboard>(result, 'dashboard', null)
      const fileName = file.name
      const dashboardFromFile: DashboardFromFile = {
        dashboard,
        fileName,
      }
      this.setState({dashboardFromFile})
    }
    fileReader.readAsText(file)
  }

  private handleImportDashboard = (e: FormEvent<HTMLFormElement>): void => {
    e.preventDefault()

    const {onImportDashboard, onDismissOverlay} = this.props
    const {dashboardFromFile} = this.state
    const {dashboard, fileName} = dashboardFromFile
    if (!_.isEmpty(dashboard)) {
      onImportDashboard(dashboard)
    } else {
      this.props.notify(
        notifyDashboardImportFailed(fileName, 'No dashboard found in file')
      )
    }
    onDismissOverlay()
  }
}

export default ImportDashboardOverlay
