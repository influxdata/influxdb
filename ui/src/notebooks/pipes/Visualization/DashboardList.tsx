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
  show: boolean
  query: string
  properties: ViewProperties
  onClose: () => void
}

type Props = StateProps & DispatchProps & OwnProps

const DashboardList: FC<Props> = ({
  show,
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

  useEffect(() => {
    setSelectedDashboard(null)
    setNewName(DEFAULT_DASHBOARD_NAME)
  }, [show])

  if (!show) {
    return null
  }

  const isEditingName =
    selectedDashboard && selectedDashboard.id === DashboardTemplate.id
  const changeName = evt => {
    setNewName(evt.target.value)
  }

  const nameInput = isEditingName && (
    <div className="notebook-visualization--dashboard-name-editor">
      <label>New Dashboard Name</label>
      <Input
        type={InputType.Text}
        placeholder="Add dashboard name"
        name="dashboardName"
        value={newName}
        onChange={changeName}
        testID="save-as-dashboard-cell--dashboard-name"
      />
    </div>
  )

  const dropdownItems = [
    {...DashboardTemplate, name: newName},
    ...dashboards,
  ].map(d => (
    <Dropdown.Item
      id={d.id}
      key={d.id}
      value={d}
      onClick={() => {
        setSelectedDashboard(d)
      }}
      selected={selectedDashboard && selectedDashboard.id === d.id}
    >
      {d.name}
    </Dropdown.Item>
  ))
  const dropdown = !isEditingName && (
    <div className="notebook-visualization--dashboard-list-dropdown">
      <Dropdown
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            testID="save-as-dashboard-cell--dropdown"
          >
            {selectedDashboard ? selectedDashboard.name : 'Select a Dashboard'}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu
            onCollapse={onCollapse}
            testID="save-as-dashboard-cell--dropdown-menu"
          >
            {dropdownItems}
          </Dropdown.Menu>
        )}
      />
    </div>
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
      createViewAndDashboard(org.id, selectedDashboard.name, view)
    } else {
      createView(selectedDashboard.id, view)
    }

    onClose()
  }

  return (
    <div className="notebook-visualization--dashboard-list">
      <h1>choose a dashboard to export to</h1>
      {dropdown}
      {nameInput}

      <div className="notebook-visualization--dashboard-list-actions">
        <SquareButton
          icon={IconFont.Checkmark}
          onClick={save}
          titleText="Save to Dashboard"
          status={saveStatus}
        />
        <SquareButton
          icon={IconFont.Remove}
          onClick={onClose}
          titleText="Cancel"
        />
      </div>
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
