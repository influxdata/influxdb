import React, {Fragment, Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import _ from 'lodash'

import HostsTable from 'src/hosts/components/HostsTable'
import SourceIndicator from 'shared/components/SourceIndicator'
import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import PageHeader from 'src/shared/components/PageHeader'

import {getCpuAndLoadForHosts, getLayouts, getAppsForHosts} from '../apis'
import {getEnv} from 'src/shared/apis/env'
import {setAutoRefresh} from 'shared/actions/app'
import {notify as notifyAction} from 'shared/actions/notifications'
import {generateForHosts} from 'src/utils/tempVars'

import {
  notifyUnableToGetHosts,
  notifyUnableToGetApps,
} from 'shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
export class HostsPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      hosts: {},
      hostsLoading: true,
      hostsError: '',
    }
  }

  async fetchHostsData() {
    const {source, links, notify} = this.props
    const {telegrafSystemInterval} = await getEnv(links.environment)
    const hostsError = notifyUnableToGetHosts().message
    const tempVars = generateForHosts(source)

    try {
      const hosts = await getCpuAndLoadForHosts(
        source.links.proxy,
        source.telegraf,
        telegrafSystemInterval,
        tempVars
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
      notify(notifyUnableToGetHosts())
      this.setState({
        hostsError,
        hostsLoading: false,
      })
    }
  }

  async componentDidMount() {
    const {notify, autoRefresh} = this.props

    this.setState({hostsLoading: true}) // Only print this once
    const results = await getLayouts()
    const data = _.get(results, 'data')
    this.layouts = data && data.layouts
    if (!this.layouts) {
      const layoutError = notifyUnableToGetApps().message
      notify(notifyUnableToGetApps())
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
    const {source} = this.props
    const {hosts, hostsLoading, hostsError} = this.state
    return (
      <div className="page hosts-list-page">
        <PageHeader
          title="Host List"
          renderOptions={this.renderHeaderOptions}
        />
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

  renderHeaderOptions = () => {
    const {autoRefresh, onChooseAutoRefresh, onManualRefresh} = this.props

    return (
      <Fragment>
        <SourceIndicator />
        <AutoRefreshDropdown
          iconName="refresh"
          selected={autoRefresh}
          onChoose={onChooseAutoRefresh}
          onManualRefresh={onManualRefresh}
        />
      </Fragment>
    )
  }

  componentWillUnmount() {
    clearInterval(this.intervalID)
    this.intervalID = false
  }
}

const {func, shape, string, number} = PropTypes

const mapStateToProps = state => {
  const {
    app: {
      persisted: {autoRefresh},
    },
    links,
  } = state
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
  autoRefresh: number.isRequired,
  manualRefresh: number,
  onChooseAutoRefresh: func.isRequired,
  onManualRefresh: func.isRequired,
  notify: func.isRequired,
}

HostsPage.defaultProps = {
  manualRefresh: 0,
}

const mapDispatchToProps = dispatch => ({
  onChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  ManualRefresh(HostsPage)
)
