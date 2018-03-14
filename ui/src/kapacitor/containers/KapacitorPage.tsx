import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {publishNotification as publishNotificationAction} from 'src/shared/actions/notifications'

import {Source} from 'src/types'

import {
  getKapacitor,
  createKapacitor,
  updateKapacitor,
  pingKapacitor,
} from 'src/shared/apis'

import KapacitorForm from '../components/KapacitorForm'

import {
  NOTIFY_KAPACITOR_CONNECTION_FAILED,
  NOTIFY_KAPACITOR_NAME_ALREADY_TAKEN,
  NOTIFY_KAPACITOR_UPDATED,
  NOTIFY_KAPACITOR_UPDATE_FAILED,
  NOTIFY_KAPACITOR_CREATED,
  NOTIFY_KAPACITOR_CREATION_FAILED,
} from 'src/shared/copy/notifications'

export const defaultName = 'My Kapacitor'
export const kapacitorPort = '9092'

type Notification = {
  id: string
  type: string
  icon: string
  duration: number
  message: string
}

interface Kapacitor {
  url: string
  name: string
  username: string
  password: string
  active: boolean
  links: {
    self: string
  }
}

interface Props {
  source: Source
  publishNotification: (notificationMessage: (notification: Notification)) => Notification
  kapacitor: Kapacitor
  router: {push: (url: string) => void}
  location: {pathname: string; hash: string}
  params: {id: string; hash: string}
}

interface State {
  kapacitor: Kapacitor
  exists: boolean
}

export class KapacitorPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      kapacitor: {
        url: this.parseKapacitorURL(),
        name: defaultName,
        username: '',
        password: '',
        active: false,
        links: {
          self: '',
        },
      },
      exists: false,
    }

    this.handleSubmit = this.handleSubmit.bind(this)
  }

  async componentDidMount() {
    const {source, params: {id}, publishNotification} = this.props
    if (!id) {
      return
    }

    try {
      const kapacitor = await getKapacitor(source, id)
      this.setState({kapacitor})
      await this.checkKapacitorConnection(kapacitor)
    } catch (err) {
      console.error('Could not get kapacitor: ', err)
      publishNotification(NOTIFY_KAPACITOR_CONNECTION_FAILED)
    }
  }

  handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {value, name} = e.target

    this.setState(prevState => {
      const update = {[name]: value}
      return {kapacitor: {...prevState.kapacitor, ...update}}
    })
  }

  handleChangeUrl = e => {
    this.setState({kapacitor: {...this.state.kapacitor, url: e.target.value}})
  }

  handleSubmit = async e => {
    e.preventDefault()
    const {
      publishNotification,
      source,
      source: {kapacitors = []},
      params,
      router,
    } = this.props

    const {kapacitor} = this.state
    kapacitor.name = kapacitor.name.trim()
    const isNameTaken = kapacitors.some(k => k.name === kapacitor.name)
    const isNew = !params.id

    if (isNew && isNameTaken) {
      publishNotification(NOTIFY_KAPACITOR_NAME_ALREADY_TAKEN)
      return
    }

    if (params.id) {
      try {
        const {data} = await updateKapacitor(kapacitor)
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        publishNotification(NOTIFY_KAPACITOR_UPDATED)
      } catch (error) {
        console.error(error)
        publishNotification(NOTIFY_KAPACITOR_UPDATE_FAILED)
      }
    } else {
      try {
        const {data} = await createKapacitor(source, kapacitor)
        // need up update kapacitor with info from server to AlertOutputs
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        router.push(`/sources/${source.id}/kapacitors/${data.id}/edit`)
        publishNotification(NOTIFY_KAPACITOR_CREATED)
      } catch (error) {
        console.error(error)
        publishNotification(NOTIFY_KAPACITOR_CREATION_FAILED)
      }
    }
  }

  handleResetToDefaults = e => {
    e.preventDefault()
    const defaultState = {
      url: this.parseKapacitorURL(),
      name: defaultName,
      username: '',
      password: '',
      active: false,
      links: {
        self: '',
      },
    }

    this.setState({kapacitor: {...defaultState}})
  }

  private checkKapacitorConnection = async (kapacitor: Kapacitor) => {
    try {
      await pingKapacitor(kapacitor)
      this.setState({exists: true})
    } catch (error) {
      this.setState({exists: false})
      this.props.publishNotification(NOTIFY_KAPACITOR_CONNECTION_FAILED)
    }
  }

  private parseKapacitorURL = () => {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  }

  render() {
    const {source, publishNotification, location, params} = this.props
    const hash = (location && location.hash) || (params && params.hash) || ''
    const {kapacitor, exists} = this.state

    return (
      <KapacitorForm
        hash={hash}
        source={source}
        exists={exists}
        kapacitor={kapacitor}
        onSubmit={this.handleSubmit}
        onChangeUrl={this.handleChangeUrl}
        onReset={this.handleResetToDefaults}
        onInputChange={this.handleInputChange}
      />
    )
  }
}

const mapDispatchToProps = dispatch => ({
  publishNotification: bindActionCreators(publishNotificationAction, dispatch),
})

export default connect(null, mapDispatchToProps)(withRouter(KapacitorPage))
