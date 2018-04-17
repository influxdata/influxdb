import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  removeAndLoadSources,
  fetchKapacitorsAsync,
  setActiveKapacitorAsync,
  deleteKapacitorAsync,
} from 'shared/actions/sources'
import {notify as notifyAction} from 'shared/actions/notifications'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import SourceIndicator from 'shared/components/SourceIndicator'
import InfluxTable from 'src/sources/components/InfluxTable'

import {
  notifySourceDeleted,
  notifySourceDeleteFailed,
} from 'shared/copy/notifications'

const V_NUMBER = VERSION // eslint-disable-line no-undef

@ErrorHandling
class ManageSources extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    this.props.sources.forEach(source => {
      this.props.fetchKapacitors(source)
    })
  }

  componentDidUpdate(prevProps) {
    if (prevProps.sources.length !== this.props.sources.length) {
      this.props.sources.forEach(source => {
        this.props.fetchKapacitors(source)
      })
    }
  }

  handleDeleteSource = source => () => {
    const {notify} = this.props

    try {
      this.props.removeAndLoadSources(source)
      notify(notifySourceDeleted(source.name))
    } catch (e) {
      notify(notifySourceDeleteFailed(source.name))
    }
  }

  handleSetActiveKapacitor = ({kapacitor}) => {
    this.props.setActiveKapacitor(kapacitor)
  }

  render() {
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
              handleDeleteKapacitor={deleteKapacitor}
              handleDeleteSource={this.handleDeleteSource}
              setActiveKapacitor={this.handleSetActiveKapacitor}
            />
            <p className="version-number">Chronograf Version: {V_NUMBER}</p>
          </div>
        </FancyScrollbar>
      </div>
    )
  }
}

const {array, func, shape, string} = PropTypes

ManageSources.propTypes = {
  source: shape({
    id: string.isRequired,
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
    }),
  }),
  sources: array,
  notify: func.isRequired,
  removeAndLoadSources: func.isRequired,
  fetchKapacitors: func.isRequired,
  setActiveKapacitor: func.isRequired,
  deleteKapacitor: func.isRequired,
}

const mapStateToProps = ({sources}) => ({
  sources,
})

const mapDispatchToProps = dispatch => ({
  removeAndLoadSources: bindActionCreators(removeAndLoadSources, dispatch),
  fetchKapacitors: bindActionCreators(fetchKapacitorsAsync, dispatch),
  setActiveKapacitor: bindActionCreators(setActiveKapacitorAsync, dispatch),
  deleteKapacitor: bindActionCreators(deleteKapacitorAsync, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(ManageSources)
