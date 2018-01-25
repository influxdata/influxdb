import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import {bindActionCreators} from 'redux'
import _ from 'lodash'

import HostsTable from 'src/hosts/components/HostsTable'
import SourceIndicator from 'shared/components/SourceIndicator'
import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import ManualRefresh from 'src/shared/components/ManualRefresh'

import {getCpuAndLoadForHosts, getLayouts, getAppsForHosts} from '../apis'
import {getEnv} from 'src/shared/apis/env'
import {setAutoRefresh} from 'shared/actions/app'

class HostsPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      hosts: {},
      hostsLoading: true,
      hostsError: '',
    }
  }

  async fetchHostsData() {
    const {source, links, addFlashMessage} = this.props
    const {telegrafSystemInterval} = await getEnv(links.environment)
    const hostsError = 'Unable to get hosts'
    try {
      const hosts = await getCpuAndLoadForHosts(
        source.links.proxy,
        source.telegraf,
        telegrafSystemInterval
      )
      if (!hosts) {
        throw new Error(hostsError)
      }
      const newHosts = await getAppsForHosts(
        source.links.proxy,
        hosts,
        this.layouts,
        source.telegraf
      )

      this.setState({
        hosts: newHosts,
        hostsError: '',
        hostsLoading: false,
      })
    } catch (error) {
      console.error(error)
      addFlashMessage({type: 'error', text: hostsError})
      this.setState({
        hostsError,
        hostsLoading: false,
      })
    }
  }

  async componentDidMount() {
    const {addFlashMessage, autoRefresh} = this.props

    this.setState({hostsLoading: true}) // Only print this once
    const {data} = await getLayouts()
    this.layouts = data.layouts
    if (!this.layouts) {
      const layoutError = 'Unable to get apps for hosts'
      addFlashMessage({type: 'error', text: layoutError})
      this.setState({
        hostsError: layoutError,
        hostsLoading: false,
      })
      return
    }
    await this.fetchHostsData()
    if (autoRefresh) {
      this.intervalID = setInterval(() => this.fetchHostsData(), autoRefresh)
    }
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.manualRefresh !== nextProps.manualRefresh) {
      this.fetchHostsData()
    }
    if (this.props.autoRefresh !== nextProps.autoRefresh) {
      clearInterval(this.intervalID)

      if (nextProps.autoRefresh) {
        this.intervalID = setInterval(
          () => this.fetchHostsData(),
          nextProps.autoRefresh
        )
      }
    }
  }

  render() {
    const {
      source,
      autoRefresh,
      onChooseAutoRefresh,
      onManualRefresh,
    } = this.props
    const {hosts, hostsLoading, hostsError} = this.state
    return (
      <div className="page hosts-list-page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Host List</h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
              <AutoRefreshDropdown
                iconName="refresh"
                selected={autoRefresh}
                onChoose={onChooseAutoRefresh}
                onManualRefresh={onManualRefresh}
              />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <HostsTable
                  source={source}
                  hosts={_.values(hosts)}
                  hostsLoading={hostsLoading}
                  hostsError={hostsError}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  componentWillUnmount() {
    clearInterval(this.intervalID)
    this.intervalID = false
  }
}

const {func, shape, string, number} = PropTypes

const mapStateToProps = state => {
  const {app: {persisted: {autoRefresh}}, links} = state
  return {
    links,
    autoRefresh,
  }
}

HostsPage.propTypes = {
  source: shape({
    id: string.isRequired,
    name: string.isRequired,
    type: string, // 'influx-enterprise'
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
    telegraf: string.isRequired,
  }),
  links: shape({
    environment: string.isRequired,
  }),
  addFlashMessage: func,
  autoRefresh: number.isRequired,
  manualRefresh: number,
  onChooseAutoRefresh: func.isRequired,
  onManualRefresh: func.isRequired,
}

HostsPage.defaultProps = {
  manualRefresh: 0,
}

const mapDispatchToProps = dispatch => ({
  onChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  ManualRefresh(withRouter(HostsPage))
)
