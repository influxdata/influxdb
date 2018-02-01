import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import uuid from 'node-uuid'

import Tickscript from 'src/kapacitor/components/Tickscript'
import * as kapactiorActionCreators from 'src/kapacitor/actions/view'
import * as errorActionCreators from 'shared/actions/errors'
import {getActiveKapacitor} from 'src/shared/apis'
import {getLogStreamByRuleID, pingKapacitorVersion} from 'src/kapacitor/apis'
import {publishNotification} from 'shared/actions/notifications'

class TickscriptPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      kapacitor: {},
      task: {
        id: '',
        name: '',
        status: 'enabled',
        tickscript: '',
        dbrps: [],
        type: 'stream',
      },
      validation: '',
      isEditingID: true,
      logs: [],
      areLogsEnabled: false,
      failStr: '',
      unsavedChanges: false,
    }
  }

  fetchChunkedLogs = async (kapacitor, ruleID) => {
    const {notify} = this.props

    try {
      const version = await pingKapacitorVersion(kapacitor)

      if (version && parseInt(version.split('.')[1], 10) < 4) {
        this.setState({
          areLogsEnabled: false,
        })
        notify(
          'warning',
          'Could not use logging, requires Kapacitor version 1.4',
          {once: true}
        )
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
          console.warn(err, failStr)
          this.setState({
            logs: [...logs, ...this.state.logs],
            failStr,
          })
        }
      }
    } catch (error) {
      console.error(error)
      notify('error', error)
      throw error
    }
  }

  async componentDidMount() {
    const {
      source,
      errorActions,
      kapacitorActions,
      params: {ruleID},
    } = this.props

    const kapacitor = await getActiveKapacitor(source)
    if (!kapacitor) {
      errorActions.errorThrown(
        'We could not find a configured Kapacitor for this source'
      )
    }

    if (this._isEditing()) {
      await kapacitorActions.getRule(kapacitor, ruleID)
      const {id, name, tickscript, dbrps, type} = this.props.rules.find(
        r => r.id === ruleID
      )

      this.setState({task: {tickscript, dbrps, type, status, name, id}})
    }

    this.fetchChunkedLogs(kapacitor, ruleID)

    this.setState({kapacitor})
  }

  componentWillUnmount() {
    this.setState({
      areLogsEnabled: false,
    })
  }

  handleSave = async () => {
    const {kapacitor, task} = this.state
    const {
      source: {id: sourceID},
      router,
      kapacitorActions: {createTask, updateTask},
      params: {ruleID},
    } = this.props

    let response

    try {
      if (this._isEditing()) {
        response = await updateTask(kapacitor, task, ruleID, router, sourceID)
      } else {
        response = await createTask(kapacitor, task, router, sourceID)
      }
      if (response && response.code === 500) {
        // check responses on failing??!
        return this.setState({validation: response.message})
      }
      this.setState({unsavedChanges: false})
    } catch (error) {
      console.error(error)
      throw error
    }
  }

  handleSaveAndExit = async () => {
    const {kapacitor, task} = this.state
    const {
      source: {id: sourceID},
      router,
      kapacitorActions: {createTaskExit, updateTaskExit},
      params: {ruleID},
    } = this.props

    let response

    try {
      if (this._isEditing()) {
        response = await updateTaskExit(
          kapacitor,
          task,
          ruleID,
          router,
          sourceID
        )
      } else {
        response = await createTaskExit(kapacitor, task, router, sourceID)
      }
      if (response && response.code === 500) {
        return this.setState({validation: response.message})
      }
    } catch (error) {
      console.error(error)
      throw error
    }
  }

  handleChangeScript = tickscript => {
    this.setState({
      task: {...this.state.task, tickscript},
      unsavedChanges: true,
    })
  }

  handleSelectDbrps = dbrps => {
    this.setState({task: {...this.state.task, dbrps}})
  }

  handleChangeType = type => () => {
    this.setState({task: {...this.state.task, type}})
  }

  handleChangeID = e => {
    this.setState({task: {...this.state.task, id: e.target.value}})
  }

  handleToggleLogsVisibility = () => {
    this.setState({areLogsVisible: !this.state.areLogsVisible})
  }

  render() {
    const {source} = this.props
    const {
      task,
      validation,
      logs,
      areLogsVisible,
      areLogsEnabled,
      unsavedChanges,
    } = this.state

    return (
      <Tickscript
        task={task}
        logs={logs}
        source={source}
        validation={validation}
        onSave={this.handleSave}
        unsavedChanges={unsavedChanges}
        onSaveAndExit={this.handleSaveAndExit}
        isNewTickscript={!this._isEditing()}
        onSelectDbrps={this.handleSelectDbrps}
        onChangeScript={this.handleChangeScript}
        onChangeType={this.handleChangeType}
        onChangeID={this.handleChangeID}
        areLogsVisible={areLogsVisible}
        areLogsEnabled={areLogsEnabled}
        onToggleLogsVisibility={this.handleToggleLogsVisibility}
      />
    )
  }

  _isEditing() {
    const {params} = this.props
    return params.ruleID && params.ruleID !== 'new'
  }
}

const {arrayOf, func, shape, string} = PropTypes

TickscriptPage.propTypes = {
  source: shape({
    name: string,
  }),
  errorActions: shape({
    errorThrown: func.isRequired,
  }).isRequired,
  kapacitorActions: shape({
    updateTask: func.isRequired,
    createTask: func.isRequired,
    updateTaskExit: func.isRequired,
    createTaskExit: func.isRequired,
    getRule: func.isRequired,
  }),
  router: shape({
    push: func.isRequired,
  }).isRequired,
  params: shape({
    ruleID: string,
  }).isRequired,
  rules: arrayOf(shape()),
  notify: func.isRequired,
}

const mapStateToProps = state => {
  return {
    rules: Object.values(state.rules),
  }
}

const mapDispatchToProps = dispatch => ({
  kapacitorActions: bindActionCreators(kapactiorActionCreators, dispatch),
  errorActions: bindActionCreators(errorActionCreators, dispatch),
  notify: bindActionCreators(publishNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TickscriptPage)
