import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import * as sourcesActions from 'src/shared/actions/sources'
import * as servicesActions from 'src/shared/actions/services'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import InfluxTable from 'src/sources/components/InfluxTable'

import {
  notifySourceDeleted,
  notifySourceDeleteFailed,
} from 'src/shared/copy/notifications'

import {Source, Notification, Service} from 'src/types'
import {getDeep} from 'src/utils/wrappers'

interface Props {
  source: Source
  sources: Source[]
  services: Service[]
  notify: (n: Notification) => void
  removeAndLoadSources: actions.RemoveAndLoadSources
}

declare var VERSION: string

@ErrorHandling
class ManageSources extends PureComponent<Props> {
  public componentDidMount() {
    this.props.fetchAllServices(this.props.sources)
  }

  public render() {
    const {sources, source, deleteFlux, services} = this.props

    return (
      <div className="page" id="manage-sources-page">
        <PageHeader titleText="Configuration" sourceIndicator={true} />
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <InfluxTable
              source={source}
              sources={sources}
              services={services}
              deleteFlux={deleteFlux}
              onDeleteSource={this.handleDeleteSource}
              setActiveFlux={this.handleSetActiveFlux}
            />
            <p className="version-number">Chronograf Version: {VERSION}</p>
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private handleSetActiveFlux = async (source, service) => {
    const {services, setActiveFlux} = this.props
    const prevActiveService = services.find(s => {
      return getDeep<boolean>(s, 'metadata.active', false)
    })
    await setActiveFlux(source, service, prevActiveService)
  }

  private handleDeleteSource = (source: Source) => {
    const {notify} = this.props

    try {
      this.props.removeAndLoadSources(source)
      notify(notifySourceDeleted(source.name))
    } catch (e) {
      notify(notifySourceDeleteFailed(source.name))
    }
  }
}

const mstp = ({sources, services}) => ({
  sources,
  services,
})

const mdtp = {
  notify: notifyAction,
  removeAndLoadSources: sourcesActions.removeAndLoadSources,
  fetchKapacitors: sourcesActions.fetchKapacitorsAsync,
  setActiveKapacitor: sourcesActions.setActiveKapacitorAsync,
  deleteKapacitor: sourcesActions.deleteKapacitorAsync,
  fetchAllServices: servicesActions.fetchAllFluxServicesAsync,
  setActiveFlux: servicesActions.setActiveServiceAsync,
  deleteFlux: servicesActions.deleteServiceAsync,
}

export default connect(mstp, mdtp)(ManageSources)
