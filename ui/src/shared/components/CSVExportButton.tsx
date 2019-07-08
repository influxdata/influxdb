// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import moment from 'moment'

// Components
import {Button, ComponentStatus, IconFont} from '@influxdata/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  files: string[] | null
}

class CSVExportButton extends PureComponent<StateProps, {}> {
  public render() {
    return (
      <Button
        titleText={this.titleText}
        text="CSV"
        icon={IconFont.Download}
        onClick={this.handleClick}
        status={this.buttonStatus}
      />
    )
  }

  private get buttonStatus(): ComponentStatus {
    const {files} = this.props

    if (files) {
      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
  }

  private get titleText(): string {
    const {files} = this.props

    if (files) {
      return 'Download query results as a .CSV file'
    }

    return 'Create a query in order to download results as .CSV'
  }

  private handleClick = () => {
    const {files} = this.props
    const csv = files.join('\n\n')
    const now = moment().format('YYYY-MM-DD-HH-mm')
    const filename = `${now} Chronograf Data`

    downloadTextFile(csv, filename, '.csv', 'text/csv')
  }
}

const mstp = (state: AppState) => {
  const {
    queryResults: {files},
  } = getActiveTimeMachine(state)

  return {files}
}

export default connect<StateProps>(mstp)(CSVExportButton)
