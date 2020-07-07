// Libraries
import React, {FC, useState, useEffect} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {uniq} from 'lodash'
import {fromFlux} from '@influxdata/giraffe'

// Components
import MatchingRuleCard from 'src/alerting/components/builder/MatchingRuleCard'
import {
  SpinnerContainer,
  TechnoSpinner,
  FlexBox,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'

// Selectors & Utils
import {getOrg} from 'src/organizations/selectors'
import {ruleToDraftRule} from 'src/notifications/rules/utils'

// API
import {getNotificationRules as apiGetNotificationRules} from 'src/client'

//Types
import {
  NotificationRule,
  AppState,
  CheckTagSet,
  GenRule,
  NotificationRuleDraft,
} from 'src/types'
import {EmptyState, ComponentSize, RemoteDataState} from '@influxdata/clockface'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

interface StateProps {
  tags: CheckTagSet[]
  orgID: string
  queryResults: string[] | null
}

const CheckMatchingRulesCard: FC<StateProps> = ({
  orgID,
  tags,
  queryResults,
}) => {
  const getMatchingRules = async (): Promise<NotificationRule[]> => {
    const checkTags = tags
      .filter(t => t.key && t.value)
      .map(t => [t.key, t.value])

    const queryTags = []

    if (queryResults) {
      const joined = queryResults.join('\n\n')
      const table = fromFlux(joined).table
      const fluxGroupKeyUnion = fromFlux(joined).fluxGroupKeyUnion.filter(
        v => v !== '_start' && v !== '_stop'
      )

      fluxGroupKeyUnion.forEach(gk => {
        const values = uniq(table.getColumn(gk, 'string'))
        values.forEach(v => {
          queryTags.push([gk, v])
        })
      })
    }

    const tagsList = [...checkTags, ...queryTags].map(t => [
      'tag',
      `${t[0].trim()}:${t[1].trim()}`,
    ])

    const resp = await apiGetNotificationRules({
      query: [['orgID', orgID], ...tagsList] as any,
    })

    if (resp.status !== 200) {
      setMatchingRules({matchingRules: [], status: RemoteDataState.Error})
      return
    }

    const matchingRules: NotificationRuleDraft[] = resp.data.notificationRules.map(
      (r: GenRule) => ruleToDraftRule(r)
    )

    setMatchingRules({
      matchingRules,
      status: RemoteDataState.Done,
    })
  }

  const [{matchingRules, status}, setMatchingRules] = useState<{
    matchingRules: NotificationRuleDraft[]
    status: RemoteDataState
  }>({matchingRules: [], status: RemoteDataState.NotStarted})

  useEffect(() => {
    setMatchingRules({
      matchingRules,
      status: RemoteDataState.Loading,
    })

    getMatchingRules()
  }, [tags, queryResults])

  let contents: JSX.Element

  if (
    status === RemoteDataState.NotStarted ||
    status === RemoteDataState.Loading
  ) {
    contents = (
      <SpinnerContainer spinnerComponent={<TechnoSpinner />} loading={status} />
    )
  } else if (!matchingRules.length) {
    contents = (
      <EmptyState
        size={ComponentSize.Small}
        className="alert-builder--card__empty"
      >
        <EmptyState.Text>
          Notification Rules configured to act on tag sets matching this Alert
          Check will show up here
        </EmptyState.Text>
        <EmptyState.Text>
          Looks like no notification rules match the tag set defined in this
          Alert Check
        </EmptyState.Text>
      </EmptyState>
    )
  } else {
    contents = (
      <>
        {matchingRules.map(r => (
          <MatchingRuleCard key={r.id} rule={r} />
        ))}
      </>
    )
  }

  return (
    <BuilderCard
      testID="builder-conditions"
      className="alert-builder--card alert-builder--conditions-card"
    >
      <BuilderCard.Header title="Matching Notification Rules" />
      <BuilderCard.Body addPadding={true} autoHideScrollbars={true}>
        <FlexBox
          direction={FlexDirection.Column}
          alignItems={AlignItems.Stretch}
          margin={ComponentSize.Medium}
        >
          {contents}
        </FlexBox>
      </BuilderCard.Body>
    </BuilderCard>
  )
}

const mstp = (state: AppState) => {
  const {
    alertBuilder: {tags},
  } = state
  const {id: orgID} = getOrg(state)

  const {
    queryResults: {files},
  } = getActiveTimeMachine(state)

  return {tags, orgID, queryResults: files}
}

export default connector(CheckMatchingRulesCard)
