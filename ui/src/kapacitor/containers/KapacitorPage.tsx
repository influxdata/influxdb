import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter} from 'react-router'

import {Source} from 'src/types'

import {
  getKapacitor,
  createKapacitor,
  updateKapacitor,
  pingKapacitor,
} from 'src/shared/apis'

import KapacitorForm from '../components/KapacitorForm'

const defaultName = 'My Kapacitor'
const kapacitorPort = '9092'

type FlashMessage = {type: string; text: string}

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
  addFlashMessage: (message: FlashMessage) => void
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
    const {source, params: {id}, addFlashMessage} = this.props
    if (!id) {
      return
    }

    try {
      const kapacitor = await getKapacitor(source, id)
      this.setState({kapacitor})
      await this.checkKapacitorConnection(kapacitor)
    } catch (err) {
      console.error('Could not get kapacitor: ', err)
      addFlashMessage({
        type: 'error',
        text: 'Could not connect to Kapacitor',
      })
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

  handleSubmit = e => {
    e.preventDefault()
    const {
      addFlashMessage,
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
      addFlashMessage({
        type: 'error',
        text: `There is already a Kapacitor configuration named "${kapacitor.name}"`,
      })
      return
    }

    if (params.id) {
      try {
        const {data} = await updateKapacitor(kapacitor)
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        addFlashMessage({type: 'success', text: 'Kapacitor Updated!'})
      } catch (error) {
        console.error(error)
        addFlashMessage({
          type: 'error',
          text: 'There was a problem updating the Kapacitor record',
        })
      }
    } else {
      try {
        const {data} = await createKapacitor(source, kapacitor)
        // need up update kapacitor with info from server to AlertOutputs
        this.setState({kapacitor: data})
        this.checkKapacitorConnection(data)
        router.push(`/sources/${source.id}/kapacitors/${data.id}/edit`)
        addFlashMessage({
          type: 'success',
          text: 'Kapacitor Created! Configuring endpoints is optional.',
        })
      } catch (error) {
        console.error(error)
        addFlashMessage({
          type: 'error',
          text: 'There was a problem creating the Kapacitor record',
        })
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
      this.props.addFlashMessage({
        type: 'error',
        text: 'Could not connect to Kapacitor. Check settings.',
      })
    }
  }

  private parseKapacitorURL = () => {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${kapacitorPort}`
  }

  render() {
    const {source, addFlashMessage, location, params} = this.props
    const hash = (location && location.hash) || (params && params.hash) || ''
    const {kapacitor, exists} = this.state

    return (
      <KapacitorForm
        hash={hash}
        source={source}
        exists={exists}
        kapacitor={kapacitor}
        onSubmit={this.handleSubmit}
        addFlashMessage={addFlashMessage}
        onChangeUrl={this.handleChangeUrl}
        onReset={this.handleResetToDefaults}
        onInputChange={this.handleInputChange}
      />
    )
  }
}

export default withRouter(KapacitorPage)
