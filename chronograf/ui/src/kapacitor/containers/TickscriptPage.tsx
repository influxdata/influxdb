import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import uuid from 'uuid'

import Tickscript from 'src/kapacitor/components/Tickscript'
import * as kapactiorActionCreators from 'src/kapacitor/actions/view'
import * as errorActionCreators from 'src/shared/actions/errors'
import {getActiveKapacitor} from 'src/shared/apis'
import {getLogStreamByRuleID, pingKapacitorVersion} from 'src/kapacitor/apis'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {
  Source,
  Kapacitor,
  Task,
  AlertRule,
  Notification,
  NotificationFunc,
} from 'src/types'
import {LogItem, DBRP} from 'src/types/kapacitor'
import {
  notifyTickscriptLoggingUnavailable,
  notifyTickscriptLoggingError,
  notifyKapacitorNotFound,
} from 'src/shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface TaskResponse {
  id: number
  code: number
  message: string
}

interface ErrorActions {
  errorThrown: (notify: string | object) => void
}

interface Router {
  push: (path: string) => void
}

interface KapacitorActions {
  updateTask: (
    kapacitor: Kapacitor,
    task: Task,
    ruleID: string,
    router: Router,
    sourceID: string
  ) => Promise<TaskResponse>
  createTask: (
    kapacitor: Kapacitor,
    task: Task,
    router: Router,
    sourceID: string
  ) => Promise<TaskResponse>
  getRule: (kapacitor: Kapacitor, ruleID: string) => void
}

interface Params {
  ruleID: string
}

interface Props {
  source: Source
  errorActions: ErrorActions
  kapacitorActions: KapacitorActions
  router: Router
  params: Params
  rules: AlertRule[]
  notify: (message: Notification | NotificationFunc) => void
}

interface State {
  kapacitor: Kapacitor
  task: Task
  consoleMessage: string
  isEditingID: boolean
  logs: LogItem[]
  areLogsVisible: boolean
  areLogsEnabled: boolean
  failStr: string
  unsavedChanges: boolean
}

@ErrorHandling
export class TickscriptPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      kapacitor: {
        id: '',
        url: '',
        name: '',
        active: false,
        insecureSkipVerify: false,
        links: {
          self: '',
        },
      },
      task: {
        id: '',
        name: '',
        status: 'enabled',
        tickscript: '',
        dbrps: [],
        type: 'stream',
      },
      consoleMessage: '',
      isEditingID: true,
      logs: [],
      failStr: '',
      areLogsVisible: false,
      areLogsEnabled: false,
      unsavedChanges: false,
    }
  }

  public async componentDidMount() {
    const {
      source,
      errorActions,
      kapacitorActions,
      params: {ruleID},
    } = this.props

    const kapacitor = await getActiveKapacitor(source)
    if (!kapacitor) {
      errorActions.errorThrown(notifyKapacitorNotFound())
    }

    if (this.isEditing) {
      await kapacitorActions.getRule(kapacitor, ruleID)
      const {id, name, tickscript, status, dbrps, type} = this.props.rules.find(
        r => r.id === ruleID
      )

      this.setState({task: {tickscript, dbrps, type, status, name, id}})
    }

    this.fetchChunkedLogs(kapacitor, ruleID)

    this.setState({kapacitor})
  }

  public componentWillUnmount() {
    this.setState({
      areLogsEnabled: false,
    })
  }

  public render() {
    const {
      task,
      logs,
      areLogsVisible,
      areLogsEnabled,
      unsavedChanges,
      consoleMessage,
    } = this.state

    return (
      <Tickscript
        task={task}
        logs={logs}
        onSave={this.handleSave}
        onExit={this.handleExit}
        unsavedChanges={unsavedChanges}
        areLogsVisible={areLogsVisible}
        areLogsEnabled={areLogsEnabled}
        consoleMessage={consoleMessage}
        onChangeID={this.handleChangeID}
        onChangeType={this.handleChangeType}
        isNewTickscript={!this.isEditing}
        onSelectDbrps={this.handleSelectDbrps}
        onChangeScript={this.handleChangeScript}
        onToggleLogsVisibility={this.handleToggleLogsVisibility}
      />
    )
  }

  private async updateTask(): Promise<TaskResponse> {
    const {kapacitor, task} = this.state
    const {
      source: {id: sourceID},
      router,
      kapacitorActions: {updateTask},
      params: {ruleID},
    } = this.props

    return await updateTask(kapacitor, task, ruleID, router, sourceID)
  }

  private async createTask(): Promise<TaskResponse> {
    const {kapacitor, task} = this.state

    const {
      source: {id: sourceID},
      router,
      kapacitorActions: {createTask},
    } = this.props

    return await createTask(kapacitor, task, router, sourceID)
  }

  private async persist(): Promise<TaskResponse> {
    if (this.isEditing) {
      return await this.updateTask()
    } else {
      return await this.createTask()
    }
  }

  private handleSave = async () => {
    const {
      source: {id: sourceID},
      router,
    } = this.props

    try {
      const response = await this.persist()

      if (response.code === 422) {
        this.setState({unsavedChanges: true, consoleMessage: response.message})
        return
      }

      if (response.code) {
        this.setState({unsavedChanges: true, consoleMessage: response.message})
      } else {
        this.setState({unsavedChanges: false, consoleMessage: ''})
      }

      router.push(`/sources/${sourceID}/tickscript/${response.id}`)
    } catch (error) {
      console.error(error)
      throw error
    }
  }

  private handleExit = () => {
    const {
      source: {id: sourceID},
      router,
    } = this.props

    return router.push(`/sources/${sourceID}/alert-rules`)
  }

  private handleChangeScript = (tickscript: string) => {
    this.setState({
      task: {...this.state.task, tickscript},
      unsavedChanges: true,
    })
  }

  private handleSelectDbrps = (dbrps: DBRP[]) => {
    this.setState({task: {...this.state.task, dbrps}, unsavedChanges: true})
  }

  private handleChangeType = (type: string) => () => {
    this.setState({task: {...this.state.task, type}, unsavedChanges: true})
  }

  private handleChangeID = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({
      task: {...this.state.task, id: e.target.value},
      unsavedChanges: true,
    })
  }

  private handleToggleLogsVisibility = () => {
    this.setState({areLogsVisible: !this.state.areLogsVisible})
  }

  private get isEditing() {
    const {params} = this.props
    return params.ruleID && params.ruleID !== 'new'
  }

  private fetchChunkedLogs = async (kapacitor, ruleID) => {
    const {notify} = this.props

    try {
      const version = await pingKapacitorVersion(kapacitor)

      if (version && parseInt(version.split('.')[1], 10) < 4) {
        this.setState({
          areLogsEnabled: false,
        })
        notify(notifyTickscriptLoggingUnavailable())
        return
      }

      if (this.state.logs.length === 0) {
        this.setState({
          areLogsEnabled: true,
          logs: [
            {
              id: uuid.v4(),
              key: uuid.v4(),
              lvl: 'info',
              msg: 'created log session',
              service: 'sessions',
              tags: 'nil',
              ts: new Date().toISOString(),
            },
          ],
        })
      }

      const response = await getLogStreamByRuleID(kapacitor, ruleID)

      const reader = await response.body.getReader()
      const decoder = new TextDecoder()

      let result

      while (this.state.areLogsEnabled === true && !(result && result.done)) {
        result = await reader.read()

        const chunk = decoder.decode(result.value || new Uint8Array(), {
          stream: !result.done,
        })

        const json = chunk.split('\n')

        let logs = []
        let failStr = this.state.failStr

        try {
          for (let objStr of json) {
            objStr = failStr + objStr
            failStr = objStr
            const jsonStr = `[${objStr.split('}{').join('},{')}]`
            logs = [
              ...logs,
              ...JSON.parse(jsonStr).map(log => ({
                ...log,
                key: uuid.v4(),
              })),
            ]
            failStr = ''
          }

          this.setState({
            logs: [...logs, ...this.state.logs],
            failStr,
          })
        } catch (err) {
          this.setState({
            logs: [...logs, ...this.state.logs],
            failStr,
          })
        }
      }
    } catch (error) {
      console.error(error)
      notify(notifyTickscriptLoggingError())
      throw error
    }
  }
}

const mapStateToProps = state => {
  return {
    rules: Object.values(state.rules),
  }
}

const mapDispatchToProps = dispatch => ({
  kapacitorActions: bindActionCreators(kapactiorActionCreators, dispatch),
  errorActions: bindActionCreators(errorActionCreators, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TickscriptPage)
