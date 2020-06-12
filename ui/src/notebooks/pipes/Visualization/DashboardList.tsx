import React, {FC, useEffect, useState, useContext} from 'react'
import {connect} from 'react-redux'
import {AppSettingContext} from 'src/notebooks/context/app'
import {getDashboards} from 'src/dashboards/actions/thunks'
import {
  createCellWithView,
  createDashboardWithView,
} from 'src/cells/actions/thunks'
import {getAll} from 'src/resources/selectors'
import {
  AppState,
  Dashboard,
  View,
  ViewProperties,
  ResourceType,
} from 'src/types'
import {
  DashboardTemplate,
  DEFAULT_DASHBOARD_NAME,
} from 'src/dashboards/constants'
import {
  Dropdown,
  Input,
  InputType,
  SquareButton,
  IconFont,
  ComponentStatus,
  ComponentColor,
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
  DropdownMenuTheme,
  InputLabel,
} from '@influxdata/clockface'

interface StateProps {
  dashboards: Dashboard[]
}

interface DispatchProps {
  loadDashboards: typeof getDashboards
  createViewAndDashboard: typeof createDashboardWithView
  createView: typeof createCellWithView
}

interface OwnProps {
  query: string
  properties: ViewProperties
  onClose: () => void
}

type Props = StateProps & DispatchProps & OwnProps

const DashboardList: FC<Props> = ({
  query,
  properties,
  onClose,
  dashboards,
  loadDashboards,
  createView,
  createViewAndDashboard,
}) => {
  const [selectedDashboard, setSelectedDashboard] = useState(null)
  const [newName, setNewName] = useState(DEFAULT_DASHBOARD_NAME)
  const {org} = useContext(AppSettingContext)

  useEffect(() => {
    loadDashboards()
  }, [])

  const isEditingName =
    selectedDashboard && selectedDashboard.id === DashboardTemplate.id
  const changeName = evt => {
    setNewName(evt.target.value)
  }

  const nameInput = isEditingName && (
    <div className="notebook-visualization--dashboard-list-section">
      <InputLabel className="notebook-visualization--dashboard-list-label">
        New Dashboard Name
      </InputLabel>
      <Input
        type={InputType.Text}
        placeholder="Name new dashboard"
        name="dashboardName"
        value={newName}
        onChange={changeName}
        autoFocus={true}
      />
    </div>
  )

  const dropdownItems = dashboards.map(d => (
    <Dropdown.Item
      id={d.id}
      key={d.id}
      value={d}
      onClick={setSelectedDashboard}
      selected={selectedDashboard && selectedDashboard.id === d.id}
    >
      {d.name}
    </Dropdown.Item>
  ))

  const dropdown = (
    <Dropdown.Menu scrollToSelected={false} theme={DropdownMenuTheme.Sapphire}>
      <Dropdown.Item
        value={DashboardTemplate}
        onClick={setSelectedDashboard}
        selected={
          selectedDashboard && selectedDashboard.id === DashboardTemplate.id
        }
      >
        Create a New Dashboard
      </Dropdown.Item>
      <Dropdown.Divider />
      {dropdownItems}
    </Dropdown.Menu>
  )

  const saveStatus = selectedDashboard
    ? ComponentStatus.Default
    : ComponentStatus.Disabled
  const save = () => {
    const view = {
      name: 'From Flow', // TODO: move meta.name to pipe.name so that we can route the name through
      properties: {
        ...properties,
        queries: [
          {
            text: query,
            editMode: 'advanced',
            name: '',
          },
        ],
      },
    } as View

    if (selectedDashboard.id === DashboardTemplate.id) {
      if (org.id) {
        createViewAndDashboard(org.id, selectedDashboard.name, view)
      }
    } else {
      createView(selectedDashboard.id, view)
    }

    onClose()
  }

  return (
    <div className="notebook-visualization--dashboard-list">
      <h4>Export Visualization</h4>
      <InputLabel className="notebook-visualization--dashboard-list-label">
        Choose a Dashboard
      </InputLabel>
      {dropdown}
      {nameInput}
      <FlexBox
        alignItems={AlignItems.Center}
        stretchToFitWidth={true}
        margin={ComponentSize.Medium}
        justifyContent={JustifyContent.Center}
        className="notebook-visualization--dashboard-list-section"
      >
        <SquareButton
          icon={IconFont.Remove}
          onClick={onClose}
          titleText="Cancel"
        />
        <SquareButton
          icon={IconFont.Checkmark}
          onClick={save}
          titleText="Save to Dashboard"
          status={saveStatus}
          color={ComponentColor.Success}
        />
      </FlexBox>
    </div>
  )
}

export {DashboardList}

const mstp = (state: AppState): StateProps => {
  const dashboards = getAll<Dashboard>(state, ResourceType.Dashboards)

  return {
    dashboards,
  }
}

const mdtp: DispatchProps = {
  loadDashboards: getDashboards,
  createView: createCellWithView,
  createViewAndDashboard: createDashboardWithView,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardList)
