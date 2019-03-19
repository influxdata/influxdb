import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Utils
import {templateToExport} from 'src/shared/utils/resourceToTemplate'

// APIs
import {client} from 'src/utils/api'
import {DocumentCreate} from '@influxdata/influx'

interface State {
  template: DocumentCreate
}

interface Props extends WithRouterProps {
  params: {orgID: string; id: string}
}

class TemplateExportOverlay extends PureComponent<Props, State> {
  public state: State = {template: null}

  public async componentDidMount() {
    const {
      params: {id},
    } = this.props
    try {
      const templateDocument = await client.templates.get(id)
      const template = templateToExport(templateDocument)
      this.setState({template})
    } catch (error) {
      console.error(error)
    }
  }

  public render() {
    const {template} = this.state
    const {
      params: {orgID},
    } = this.props

    if (!template) {
      return null
    }

    return (
      <ExportOverlay
        resourceName="Template"
        resource={template}
        onDismissOverlay={this.onDismiss}
        orgID={orgID}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter(TemplateExportOverlay)
