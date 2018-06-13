import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import * as actions from 'src/shared/actions/sources'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import SourceIndicator from 'src/shared/components/SourceIndicator'
import InfluxTable from 'src/sources/components/InfluxTable'

import {
  notifySourceDeleted,
  notifySourceDeleteFailed,
} from 'src/shared/copy/notifications'

import {Source, NotificationFunc} from 'src/types'

interface Props {
  source: Source
  sources: Source[]
  notify: NotificationFunc
  deleteKapacitor: actions.DeleteKapacitorAsync
  fetchKapacitors: actions.FetchKapacitorsAsync
  removeAndLoadSources: actions.RemoveAndLoadSources
  setActiveKapacitor: actions.SetActiveKapacitorAsync
}

declare var VERSION: string

@ErrorHandling
class ManageSources extends PureComponent<Props> {
  public componentDidMount() {
    this.props.sources.forEach(source => {
      this.props.fetchKapacitors(source)
    })
  }

  public componentDidUpdate(prevProps: Props) {
    if (prevProps.sources.length !== this.props.sources.length) {
      this.props.sources.forEach(source => {
        this.props.fetchKapacitors(source)
      })
    }
  }

  public render() {
    const {sources, source, deleteKapacitor} = this.props

    return (
      <div className="page" id="manage-sources-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Configuration</h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <InfluxTable
              source={source}
              sources={sources}
              deleteKapacitor={deleteKapacitor}
              onDeleteSource={this.handleDeleteSource}
              setActiveKapacitor={this.handleSetActiveKapacitor}
            />
            <p className="version-number">Chronograf Version: {VERSION}</p>
          </div>
        </FancyScrollbar>
      </div>
    )
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

  private handleSetActiveKapacitor = ({kapacitor}) => {
    this.props.setActiveKapacitor(kapacitor)
  }
}

const mstp = ({sources}) => ({
  sources,
})

const mdtp = {
  removeAndLoadSources: actions.removeAndLoadSources,
  fetchKapacitors: actions.fetchKapacitorsAsync,
  setActiveKapacitor: actions.setActiveKapacitorAsync,
  deleteKapacitor: actions.deleteKapacitorAsync,
  notify: notifyAction,
}

export default connect(mstp, mdtp)(ManageSources)
