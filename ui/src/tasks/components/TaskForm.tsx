// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

// APIs
import {client} from 'src/utils/api'

// Components
import {
  Columns,
  Button,
  ButtonShape,
  ComponentColor,
  ButtonType,
} from '@influxdata/clockface'
import {ComponentSpacer, Form, Grid, Input, Radio} from 'src/clockface'
import TaskOptionsOrgDropdown from 'src/tasks/components/TasksOptionsOrgDropdown'
import TaskOptionsOrgIDDropdown from 'src/tasks/components/TasksOptionsOrgIDDropdown'
import TaskScheduleFormField from 'src/tasks/components/TaskScheduleFormField'
import TaskOptionsBucketDropdown from 'src/tasks/components/TasksOptionsBucketDropdown'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

// Types
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Alignment, Stack, ComponentStatus} from 'src/clockface/types'
import {Organization, Bucket} from '@influxdata/influx'

// Styles
import './TaskForm.scss'

interface Props {
  orgs: Organization[]
  taskOptions: TaskOptions
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeTaskOrgID: (orgID: string) => void
  onChangeToOrgName: (orgName: string) => void
  onChangeToBucketName: (bucketName: string) => void
  isInOverlay?: boolean
  onSubmit?: () => void
  canSubmit?: boolean
  dismiss?: () => void
}

interface State {
  schedule: TaskSchedule
}

const getBuckets = (org: Organization) => client.buckets.getAllByOrg(org.name)

export default class TaskForm extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    isInOverlay: false,
    onSubmit: () => {},
    canSubmit: true,
    dismiss: () => {},
  }
  constructor(props: Props) {
    super(props)

    this.state = {
      schedule: props.taskOptions.taskScheduleType,
    }
  }

  public render() {
    const {
      onChangeInput,
      onChangeTaskOrgID,
      onChangeToOrgName,
      onChangeToBucketName,
      taskOptions: {
        name,
        taskScheduleType,
        interval,
        offset,
        cron,
        orgID,
        toOrgName,
        toBucketName,
      },
      orgs,
      isInOverlay,
    } = this.props

    return (
      <Form>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Name">
                <Input
                  name="name"
                  placeholder="Name your task"
                  onChange={onChangeInput}
                  value={name}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Owner">
                <TaskOptionsOrgIDDropdown
                  orgs={orgs}
                  selectedOrgID={orgID}
                  onChangeOrgID={onChangeTaskOrgID}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column>
              <Form.Element label="Schedule Task">
                <ComponentSpacer
                  align={Alignment.Left}
                  stackChildren={Stack.Rows}
                >
                  <Radio shape={ButtonShape.StretchToFit}>
                    <Radio.Button
                      id="every"
                      active={taskScheduleType === TaskSchedule.interval}
                      value={TaskSchedule.interval}
                      titleText="Run task at regular intervals"
                      onClick={this.handleChangeScheduleType}
                    >
                      Every
                    </Radio.Button>
                    <Radio.Button
                      id="cron"
                      active={taskScheduleType === TaskSchedule.cron}
                      value={TaskSchedule.cron}
                      titleText="Use cron syntax for more control over scheduling"
                      onClick={this.handleChangeScheduleType}
                    >
                      Cron
                    </Radio.Button>
                  </Radio>
                  {this.cronHelper}
                </ComponentSpacer>
              </Form.Element>
            </Grid.Column>
            <TaskScheduleFormField
              onChangeInput={onChangeInput}
              schedule={taskScheduleType}
              interval={interval}
              offset={offset}
              cron={cron}
            />
            {isInOverlay && (
              <Grid.Column widthXS={Columns.Six}>
                <Form.Element label="Output Organization">
                  <TaskOptionsOrgDropdown
                    orgs={orgs}
                    selectedOrgName={toOrgName}
                    onChangeOrgName={onChangeToOrgName}
                  />
                </Form.Element>
              </Grid.Column>
            )}
            {isInOverlay && (
              <Grid.Column widthXS={Columns.Six}>
                <Form.Element label="Output Bucket">
                  <GetOrgResources<Bucket[]>
                    organization={this.toOrganization}
                    fetcher={getBuckets}
                  >
                    {(buckets, loading) => (
                      <TaskOptionsBucketDropdown
                        buckets={buckets}
                        selectedBucketName={toBucketName}
                        onChangeBucketName={onChangeToBucketName}
                        loading={loading}
                      />
                    )}
                  </GetOrgResources>
                </Form.Element>
              </Grid.Column>
            )}
            {isInOverlay && this.buttons}
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get toOrganization(): Organization {
    const {
      orgs,
      taskOptions: {toOrgName},
    } = this.props
    const toOrg = orgs.find(o => o.name === toOrgName)
    return toOrg
  }

  private get buttons(): JSX.Element {
    const {onSubmit, canSubmit, dismiss} = this.props
    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Footer>
          <Button
            text="Cancel"
            onClick={dismiss}
            titleText="Cancel save"
            type={ButtonType.Button}
          />
          <Button
            text={'Save as Task'}
            color={ComponentColor.Success}
            type={ButtonType.Submit}
            onClick={onSubmit}
            status={
              canSubmit ? ComponentStatus.Default : ComponentStatus.Disabled
            }
          />
        </Form.Footer>
      </Grid.Column>
    )
  }

  private get cronHelper(): JSX.Element {
    const {taskOptions} = this.props

    if (taskOptions.taskScheduleType === TaskSchedule.cron) {
      return (
        <Form.Box>
          <p className="time-format--helper">
            For more information on cron syntax,{' '}
            <a href="https://crontab.guru/" target="_blank">
              click here
            </a>
          </p>
        </Form.Box>
      )
    }
  }

  private handleChangeScheduleType = (schedule: TaskSchedule): void => {
    this.props.onChangeScheduleType(schedule)
  }
}
