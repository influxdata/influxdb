import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'
import SaveAsTaskForm from 'src/dataExplorer/components/SaveAsTaskForm'
import SaveAsVariable from 'src/dataExplorer/components/SaveAsVariable'
import {Radio, Overlay} from 'src/clockface'

// Styles
import 'src/dataExplorer/components/SaveAsButton.scss'

enum SaveAsOption {
  Dashboard = 'dashboard',
  Task = 'task',
  Variable = 'variable',
}

interface State {
  saveAsOption: SaveAsOption
}

class SaveAsOverlay extends PureComponent<WithRouterProps, State> {
  public state: State = {
    saveAsOption: SaveAsOption.Dashboard,
  }

  render() {
    const {saveAsOption} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Heading title="Save As" onDismiss={this.handleHideOverlay} />
          <Overlay.Body>
            <div className="save-as--options">
              <Radio>
                <Radio.Button
                  active={saveAsOption === SaveAsOption.Dashboard}
                  value={SaveAsOption.Dashboard}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="cell-radio-button"
                >
                  Dashboard Cell
                </Radio.Button>
                <Radio.Button
                  active={saveAsOption === SaveAsOption.Task}
                  value={SaveAsOption.Task}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="task--radio-button"
                >
                  Task
                </Radio.Button>
                <Radio.Button
                  active={saveAsOption === SaveAsOption.Variable}
                  value={SaveAsOption.Variable}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="variable-radio-button"
                >
                  Variable
                </Radio.Button>
              </Radio>
            </div>
            {this.saveAsForm}
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get saveAsForm(): JSX.Element {
    const {saveAsOption} = this.state

    if (saveAsOption === SaveAsOption.Dashboard) {
      return <SaveAsCellForm dismiss={this.handleHideOverlay} />
    } else if (saveAsOption === SaveAsOption.Task) {
      return <SaveAsTaskForm dismiss={this.handleHideOverlay} />
    } else if (saveAsOption === SaveAsOption.Variable) {
      return <SaveAsVariable onHideOverlay={this.handleHideOverlay} />
    }
  }

  private handleHideOverlay = () => {
    this.props.router.goBack()
  }

  private handleSetSaveAsOption = (saveAsOption: SaveAsOption) => {
    this.setState({saveAsOption})
  }
}

export default withRouter<{}, {}>(SaveAsOverlay)
