import React, {PureComponent, MouseEvent, ChangeEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'
import {createSource, updateSource} from 'src/shared/apis'

import {
  addSource as addSourceAction,
  updateSource as updateSourceAction,
  AddSource,
  UpdateSource,
} from 'src/shared/actions/sources'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import Notifications from 'src/shared/components/Notifications'
import SourceForm from 'src/sources/components/SourceForm'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import {DEFAULT_SOURCE} from 'src/shared/constants'

const INITIAL_PATH = '/sources/new'

import {
  notifySourceUpdated,
  notifySourceUpdateFailed,
  notifySourceCreationFailed,
  notifySourceCreationSucceeded,
} from 'src/shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source} from 'src/types'
import * as NotificationsActions from 'src/types/actions/notifications'

interface Props extends WithRouterProps {
  notify: NotificationsActions.PublishNotificationActionCreator
  addSource: AddSource
  updateSource: UpdateSource
  sourcesLink: string
  sources: Source[]
}

interface State {
  isCreated: boolean
  source: Partial<Source>
  editMode: boolean
  isInitialSource: boolean
}

@ErrorHandling
class SourcePage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isCreated: false,
      source: DEFAULT_SOURCE,
      editMode: props.params.id !== undefined,
      isInitialSource: props.router.location.pathname === INITIAL_PATH,
    }
  }

  public async componentDidMount() {
    this.setState({
      source: this.source,
    })
  }

  public render() {
    const {source, editMode, isInitialSource} = this.state

    return (
      <div className={`${isInitialSource ? '' : 'page'}`}>
        <Notifications />
        <PageHeader titleText={this.pageTitle} />
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                <div className="panel">
                  <SourceForm
                    source={source}
                    editMode={editMode}
                    onInputChange={this.handleInputChange}
                    onSubmit={this.handleSubmit}
                    onBlurSourceURL={this.handleBlurSourceURL}
                    isInitialSource={isInitialSource}
                  />
                </div>
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get source(): Partial<Source> {
    const {sources, params} = this.props
    const source = sources.find(s => s.id === params.id) || {}
    return {...DEFAULT_SOURCE, ...source}
  }

  private handleSubmit = (e: MouseEvent<HTMLFormElement>): void => {
    e.preventDefault()
    const {isCreated, editMode} = this.state
    const isNewSource = !editMode
    if (!isCreated && isNewSource) {
      return this.setState(this.normalizeSource, this.createSource)
    }
    this.setState(this.normalizeSource, this.updateSource)
  }

  private normalizeSource({source}) {
    const url = source.url.trim()
    if (source.url.startsWith('http')) {
      return {source: {...source, url}}
    }
    return {source: {...source, url: `http://${url}`}}
  }

  private createSourceOnBlur = async () => {
    const {source} = this.state
    const {sourcesLink} = this.props
    // if there is a type on source it has already been created
    if (source.type) {
      return
    }

    try {
      const sourceFromServer = await createSource(sourcesLink, source)
      this.props.addSource(sourceFromServer)
      this.setState({
        source: {...DEFAULT_SOURCE, ...sourceFromServer},
        isCreated: true,
      })
    } catch (err) {
      // dont want to flash this until they submit
      const error = this.parseError(err)
      console.error('Error creating InfluxDB connection: ', error)
    }
  }

  private createSource = async () => {
    const {source} = this.state
    const {notify, sourcesLink} = this.props

    try {
      const sourceFromServer = await createSource(sourcesLink, source)
      this.props.addSource(sourceFromServer)
      this.redirect(sourceFromServer)
      notify(notifySourceCreationSucceeded(source.name))
    } catch (err) {
      // dont want to flash this until they submit
      notify(notifySourceCreationFailed(source.name, this.parseError(err)))
    }
  }

  private updateSource = async () => {
    const {source} = this.state
    const {notify} = this.props
    try {
      const sourceFromServer = await updateSource(source)
      this.props.updateSource(sourceFromServer)
      this.redirect(sourceFromServer)
      notify(notifySourceUpdated(source.name))
    } catch (error) {
      notify(notifySourceUpdateFailed(source.name, this.parseError(error)))
    }
  }

  private redirect = source => {
    const {isInitialSource} = this.state
    const {params, router} = this.props

    if (isInitialSource) {
      return this.redirectToApp(source)
    }

    router.push(`/sources/${params.sourceID}/manage-sources`)
  }

  private parseError = (error): string => {
    return _.get(error, ['data', 'message'], error)
  }

  private redirectToApp = source => {
    const {location, router} = this.props
    const {redirectPath} = location.query

    if (!redirectPath) {
      return router.push(`/sources/${source.id}/hosts`)
    }

    const fixedPath = redirectPath.replace(
      /\/sources\/[^/]*/,
      `/sources/${source.id}`
    )
    return router.push(fixedPath)
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    let val = e.target.value
    const name = e.target.name

    if (e.target.type === 'checkbox') {
      val = e.target.checked as any
    }

    this.setState(prevState => {
      const source = {
        ...prevState.source,
        [name]: val,
      }

      return {...prevState, source}
    })
  }

  private handleBlurSourceURL = () => {
    const {source, editMode} = this.state
    if (editMode) {
      this.setState(this.normalizeSource)
      return
    }

    if (!source.url) {
      return
    }

    this.setState(this.normalizeSource, this.createSourceOnBlur)
  }

  private get pageTitle(): string {
    const {editMode} = this.state

    if (editMode) {
      return 'Configure InfluxDB Connection'
    }

    return 'Add a New InfluxDB Connection'
  }
}

const mdtp = {
  notify: notifyAction,
  addSource: addSourceAction,
  updateSource: updateSourceAction,
}

const mstp = ({links, sources}) => ({
  sourcesLink: links.sources,
  sources,
})

export default connect(mstp, mdtp)(withRouter(SourcePage))
