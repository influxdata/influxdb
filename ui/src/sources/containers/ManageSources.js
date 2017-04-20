import React, {Component, PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import _ from 'lodash'

import {removeAndLoadSources, fetchKapacitorsAsync} from 'src/shared/actions/sources'

import InfluxTable from '../components/InfluxTable'

class ManageSources extends Component {
  constructor(props) {
    super(props)

    this.handleDeleteSource = ::this.handleDeleteSource
  }

  componentDidMount() {
    this.props.sources.forEach((source) => {
      this.props.fetchKapacitors(source)
    })
  }

  componentDidUpdate(prevProps) {
    if (!_.isEqual(prevProps.sources, this.props.sources)) {
      this.props.sources.forEach((source) => {
        this.props.fetchKapacitors(source)
      })
    }
  }

  handleDeleteSource(source) {
    const {addFlashMessage} = this.props

    try {
      this.props.removeAndLoadSources(source)
    } catch (e) {
      addFlashMessage({type: 'error', text: 'Could not remove source from Chronograf'})
    }
  }

  render() {
    const {sources, source, location} = this.props

    return (
      <div className="page" id="manage-sources-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>Configuration</h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <InfluxTable
              handleDeleteSource={this.handleDeleteSource}
              source={source}
              sources={sources}
              location={location}
            />
          </div>
        </div>
      </div>
    )
  }
}

const {
  array,
  func,
  shape,
  string,
} = PropTypes

ManageSources.propTypes = {
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
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
}

const mapStateToProps = ({sources}) => ({
  sources,
})

const mapDispatchToProps = (dispatch) => ({
  removeAndLoadSources: bindActionCreators(removeAndLoadSources, dispatch),
  fetchKapacitors: bindActionCreators(fetchKapacitorsAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(ManageSources))
