// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FilterList from 'src/shared/components/Filter'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import GetResources from 'src/shared/components/GetResources'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Actions
import {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  addTaskLabelAsync,
  removeTaskLabelAsync,
  runTask,
} from 'src/tasks/actions'
import {
  checkTaskLimits as checkTasksLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'

// Types
import {AppState, Task, RemoteDataState, ResourceType} from 'src/types'
import {InjectedRouter, WithRouterProps} from 'react-router'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {extractTaskLimits} from 'src/cloud/utils/limits'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  updateTaskStatus: typeof updateTaskStatus
  updateTaskName: typeof updateTaskName
  deleteTask: typeof deleteTask
  cloneTask: typeof cloneTask
  selectTask: typeof selectTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  onAddTaskLabel: typeof addTaskLabelAsync
  onRemoveTaskLabel: typeof removeTaskLabelAsync
  onRunTask: typeof runTask
  checkTaskLimits: typeof checkTasksLimitsAction
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  status: RemoteDataState
  limitStatus: LimitStatus
}

type Props = ConnectedDispatchProps &
  PassedInProps &
  ConnectedStateProps &
  WithRouterProps

interface State {
  isImporting: boolean
  taskLabelsEdit: Task
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Task

@ErrorHandling
class TasksPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    props.setSearchTerm('')
    if (!props.showInactive) {
      props.setShowInactive()
    }

    this.state = {
      isImporting: false,
      taskLabelsEdit: null,
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render(): JSX.Element {
    const {sortKey, sortDirection, sortType} = this.state
    const {
      setSearchTerm,
      updateTaskName,
      searchTerm,
      setShowInactive,
      showInactive,
      onAddTaskLabel,
      onRemoveTaskLabel,
      onRunTask,
      checkTaskLimits,
      limitStatus,
      children,
    } = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Tasks'])}>
          <TasksHeader
            onCreateTask={this.handleCreateTask}
            setShowInactive={setShowInactive}
            showInactive={showInactive}
            onImportTask={this.summonImportOverlay}
            onImportFromTemplate={this.summonImportFromTemplateOverlay}
            limitStatus={limitStatus}
          />
          <Page.Contents fullWidth={false} scrollable={true}>
            <GetResources resources={[ResourceType.Tasks, ResourceType.Labels]}>
              <GetAssetLimits>
                <AssetLimitAlert
                  resourceName="tasks"
                  limitStatus={limitStatus}
                />
                <FilterList<Task>
                  list={this.filteredTasks}
                  searchTerm={searchTerm}
                  searchKeys={['name', 'labels[].name']}
                >
                  {ts => (
                    <TasksList
                      searchTerm={searchTerm}
                      tasks={ts}
                      totalCount={this.totalTaskCount}
                      onActivate={this.handleActivate}
                      onDelete={this.handleDelete}
                      onCreate={this.handleCreateTask}
                      onClone={this.handleClone}
                      onSelect={this.props.selectTask}
                      onAddTaskLabel={onAddTaskLabel}
                      onRemoveTaskLabel={onRemoveTaskLabel}
                      onRunTask={onRunTask}
                      onFilterChange={setSearchTerm}
                      filterComponent={this.search}
                      onUpdate={updateTaskName}
                      onImportTask={this.summonImportOverlay}
                      onImportFromTemplate={
                        this.summonImportFromTemplateOverlay
                      }
                      sortKey={sortKey}
                      sortDirection={sortDirection}
                      sortType={sortType}
                      onClickColumn={this.handleClickColumn}
                      checkTaskLimits={checkTaskLimits}
                    />
                  )}
                </FilterList>
                {this.hiddenTaskAlert}
              </GetAssetLimits>
            </GetResources>
          </Page.Contents>
        </Page>
        {children}
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    let sortType = SortTypes.String

    if (sortKey === 'latestCompleted') {
      sortType = SortTypes.Date
    }

    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private handleActivate = (task: Task) => {
    this.props.updateTaskStatus(task)
  }

  private handleDelete = (task: Task) => {
    this.props.deleteTask(task)
  }

  private handleClone = (task: Task) => {
    const {tasks} = this.props
    this.props.cloneTask(task, tasks)
  }

  private handleCreateTask = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/new`)
  }

  private summonImportFromTemplateOverlay = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/import/template`)
  }

  private summonImportOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/import`)
  }

  private get search(): JSX.Element {
    const {setSearchTerm, searchTerm} = this.props

    return (
      <SearchWidget
        placeholderText="Filter tasks..."
        onSearch={setSearchTerm}
        searchTerm={searchTerm}
      />
    )
  }

  private get filteredTasks(): Task[] {
    const {tasks, showInactive} = this.props
    const matchingTasks = tasks.filter(t => {
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === 'active'
      }

      return activeFilter
    })

    return matchingTasks
  }

  private get totalTaskCount(): number {
    return this.props.tasks.length
  }

  private get hiddenTaskAlert(): JSX.Element {
    const {showInactive, tasks} = this.props

    const hiddenCount = tasks.filter(t => t.status === 'inactive').length

    const allTasksAreHidden = hiddenCount === tasks.length

    if (allTasksAreHidden || showInactive) {
      return null
    }

    if (hiddenCount) {
      const pluralizer = hiddenCount === 1 ? '' : 's'
      const verb = hiddenCount === 1 ? 'is' : 'are'

      return (
        <div className="hidden-tasks-alert">{`${hiddenCount} inactive task${pluralizer} ${verb} hidden from view`}</div>
      )
    }
  }
}

const mstp = ({
  tasks: {status, list, searchTerm, showInactive},
  cloud: {limits},
}: AppState): ConnectedStateProps => {
  return {
    tasks: list,
    status: status,
    searchTerm,
    showInactive,
    limitStatus: extractTaskLimits(limits),
  }
}

const mdtp: ConnectedDispatchProps = {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  onRemoveTaskLabel: removeTaskLabelAsync,
  onAddTaskLabel: addTaskLabelAsync,
  onRunTask: runTask,
  checkTaskLimits: checkTasksLimitsAction,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(TasksPage)
