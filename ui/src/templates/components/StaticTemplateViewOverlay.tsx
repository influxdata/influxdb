import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import _ from 'lodash'

// Components
import ViewOverlay from 'src/shared/components/ViewOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {RemoteDataState} from '@influxdata/clockface'

import {staticTemplates} from 'src/templates/constants/defaultTemplates'
import {DashboardTemplate} from 'src/types'

interface OwnProps {
  match: {id: string}
}

type Props = OwnProps & RouteComponentProps<{orgID: string; id: string}>

@ErrorHandling
class TemplateExportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ViewOverlay
        resource={this.template}
        overlayHeading={this.overlayTitle}
        onDismissOverlay={this.onDismiss}
        status={RemoteDataState.Done}
      />
    )
  }

  private get template(): DashboardTemplate {
    const {
      match: {
        params: {id},
      },
    } = this.props

    return staticTemplates[id]
  }

  private get overlayTitle() {
    return this.template.meta.name
  }

  private onDismiss = () => {
    const {history} = this.props

    history.goBack()
  }
}

export default withRouter(TemplateExportOverlay)
