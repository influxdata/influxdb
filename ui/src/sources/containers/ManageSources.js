import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {
  removeAndLoadSources,
  fetchKapacitorsAsync,
  setActiveKapacitorAsync,
  deleteKapacitorAsync,
} from 'shared/actions/sources'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import SourceIndicator from 'shared/components/SourceIndicator'
import InfluxTable from 'src/sources/components/InfluxTable'

const V_NUMBER = VERSION // eslint-disable-line no-undef

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
    const {addFlashMessage} = this.props

    try {
      this.props.removeAndLoadSources(source)
      addFlashMessage({
        type: 'success',
        text: `Deleted source ${source.name}`,
      })
    } catch (e) {
      addFlashMessage({
        type: 'error',
        text: 'Could not remove source from Chronograf',
      })
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
            <p className="version-number">
              Chronograf Version: {V_NUMBER}
            </p>
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
  addFlashMessage: func,
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
})

export default connect(mapStateToProps, mapDispatchToProps)(ManageSources)
