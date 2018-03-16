import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {notify as notifyAction} from 'src/shared/actions/notifications'

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

export interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}

export type NotificationFunc = () => Notification

interface Kapacitor {
  url: string
  name: string
  username: string
  password: string
  active: boolean
  insecureSkipVerify: boolean
  links: {
    self: string
  }
}

interface Props {
  source: Source
  notify: (message: Notification | NotificationFunc) => void
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
      kapacitor: this.defaultKapacitor,
      exists: false
    }

    this.handleSubmit = this.handleSubmit.bind(this)
  }

  async componentDidMount() {
    const {source, params: {id}, notify} = this.props
    if (!id) {
      return
    }

    try {
      const kapacitor = await getKapacitor(source, id)
      this.setState({kapacitor})
      await this.checkKapacitorConnection(kapacitor)
    } catch (error) {
      console.error('Could not get kapacitor: ', error)
      notify(NOTIFY_KAPACITOR_CONNECTION_FAILED)
    }
  }

  handleCheckboxChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {checked} = e.target

    this.setState({
      kapacitor: {
        ...this.state.kapacitor,
        insecureSkipVerify: checked
      }
    })
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
      notify,
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
      notify(NOTIFY_KAPACITOR_NAME_ALREADY_TAKEN)
      return
    }

    if (params.id) {
      try {
        const {data} = await updateKapacitor(kapacitor)
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        notify(NOTIFY_KAPACITOR_UPDATED)
      } catch (error) {
        console.error(error)
        notify(NOTIFY_KAPACITOR_UPDATE_FAILED)
      }
    } else {
      try {
        const {data} = await createKapacitor(source, kapacitor)
        // need up update kapacitor with info from server to AlertOutputs
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        router.push(`/sources/${source.id}/kapacitors/${data.id}/edit`)
        notify(NOTIFY_KAPACITOR_CREATED)
      } catch (error) {
        console.error(error)
        notify(NOTIFY_KAPACITOR_CREATION_FAILED)
      }
    }
  }

  handleResetToDefaults = e => {
    e.preventDefault()
    this.setState({kapacitor: {...this.defaultKapacitor}})
  }

  private get defaultKapacitor() {
    return {
      url: this.parseKapacitorURL(),
      name: defaultName,
      username: '',
      password: '',
      active: false,
      insecureSkipVerify: false,
      links: {
        self: '',
      },
    }
  }

  private checkKapacitorConnection = async (kapacitor: Kapacitor) => {
    try {
      await pingKapacitor(kapacitor)
      this.setState({exists: true})
    } catch (error) {
      console.error(error)
      this.setState({exists: false})
      this.props.notify(NOTIFY_KAPACITOR_CONNECTION_FAILED)
    }
  }

  private parseKapacitorURL = () => {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  }

  render() {
    const {source, location, params, notify} = this.props
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
        notify={notify}
        onCheckboxChange={this.handleCheckboxChange}
      />
    )
  }
}

const mapDispatchToProps = dispatch => ({
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(null, mapDispatchToProps)(withRouter(KapacitorPage))
