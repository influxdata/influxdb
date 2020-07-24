import {produce} from 'immer'
import {
  Action,
  ADD_TEMPLATE_SUMMARY,
  POPULATE_TEMPLATE_SUMMARIES,
  REMOVE_TEMPLATE_SUMMARY,
  SET_COMMUNITY_TEMPLATE_TO_INSTALL,
  SET_EXPORT_TEMPLATE,
  SET_TEMPLATE_SUMMARY,
  SET_TEMPLATES_STATUS,
  TOGGLE_TEMPLATE_RESOURCE_INSTALL,
  SET_STACKS,
} from 'src/templates/actions/creators'
import {
  CommunityTemplate,
  ResourceType,
  RemoteDataState,
  TemplateSummary,
  TemplatesState,
} from 'src/types'
import {
  addResource,
  removeResource,
  setResource,
  setResourceAtID,
} from 'src/resources/reducers/helpers'

const defaultCommunityTemplate = (): CommunityTemplate => {
  return {
    sources: [],
    stackID: '',
    summary: {},
    diff: {},
    resourcesToSkip: {},
  }
}

export const defaultState = (): TemplatesState => ({
  communityTemplateToInstall: defaultCommunityTemplate(),
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  exportTemplate: {
    status: RemoteDataState.NotStarted,
    item: null,
  },
  stacks: [],
})

export const templatesReducer = (
  state: TemplatesState = defaultState(),
  action: Action
): TemplatesState =>
  produce(state, draftState => {
    switch (action.type) {
      case POPULATE_TEMPLATE_SUMMARIES: {
        setResource<TemplateSummary>(draftState, action, ResourceType.Templates)

        return
      }

      case SET_TEMPLATES_STATUS: {
        const {status} = action
        draftState.status = status
        return
      }

      case SET_TEMPLATE_SUMMARY: {
        setResourceAtID<TemplateSummary>(
          draftState,
          action,
          ResourceType.Templates
        )

        return
      }

      case SET_COMMUNITY_TEMPLATE_TO_INSTALL: {
        const {template} = action

        const communityTemplateToInstall = {
          ...defaultCommunityTemplate(),
          ...template,
        }

        communityTemplateToInstall.summary.dashboards = (
          template.summary.dashboards || []
        ).map(dashboard => {
          if (!dashboard.hasOwnProperty('shouldInstall')) {
            dashboard.shouldInstall = true
          }
          return dashboard
        })

        communityTemplateToInstall.summary.telegrafConfigs = (
          template.summary.telegrafConfigs || []
        ).map(telegrafConfig => {
          if (!telegrafConfig.hasOwnProperty('shouldInstall')) {
            telegrafConfig.shouldInstall = true
          }
          return telegrafConfig
        })

        communityTemplateToInstall.summary.buckets = (
          template.summary.buckets || []
        ).map(bucket => {
          if (!bucket.hasOwnProperty('shouldInstall')) {
            bucket.shouldInstall = true
          }
          return bucket
        })

        communityTemplateToInstall.summary.checks = (
          template.summary.checks || []
        ).map(check => {
          if (!check.hasOwnProperty('shouldInstall')) {
            check.shouldInstall = true
          }
          return check
        })

        communityTemplateToInstall.summary.variables = (
          template.summary.variables || []
        ).map(variable => {
          if (!variable.hasOwnProperty('shouldInstall')) {
            variable.shouldInstall = true
          }
          return variable
        })

        communityTemplateToInstall.summary.notificationRules = (
          template.summary.notificationRules || []
        ).map(notificationRule => {
          if (!notificationRule.hasOwnProperty('shouldInstall')) {
            notificationRule.shouldInstall = true
          }
          return notificationRule
        })

        communityTemplateToInstall.summary.notificationEndpoints = (
          template.summary.notificationEndpoints || []
        ).map(notificationEndpoint => {
          if (!notificationEndpoint.hasOwnProperty('shouldInstall')) {
            notificationEndpoint.shouldInstall = true
          }
          return notificationEndpoint
        })

        communityTemplateToInstall.summary.labels = (
          template.summary.labels || []
        ).map(label => {
          if (!label.hasOwnProperty('shouldInstall')) {
            label.shouldInstall = true
          }
          return label
        })

        communityTemplateToInstall.summary.summaryTask = (
          template.summary.summaryTask || []
        ).map(summaryTask => {
          if (!summaryTask.hasOwnProperty('shouldInstall')) {
            summaryTask.shouldInstall = true
          }
          return summaryTask
        })

        draftState.communityTemplateToInstall = communityTemplateToInstall
        return
      }

      case SET_EXPORT_TEMPLATE: {
        const {status, item} = action
        draftState.exportTemplate.status = status

        if (item) {
          draftState.exportTemplate.item = item
        } else {
          draftState.exportTemplate.item = null
        }
        return
      }

      case REMOVE_TEMPLATE_SUMMARY: {
        removeResource<TemplateSummary>(draftState, action)

        return
      }

      case ADD_TEMPLATE_SUMMARY: {
        addResource<TemplateSummary>(draftState, action, ResourceType.Templates)

        return
      }

      case TOGGLE_TEMPLATE_RESOURCE_INSTALL: {
        const {resourceType, shouldInstall, templateMetaName} = action

        const templateToInstall = {...draftState.communityTemplateToInstall}

        templateToInstall.summary[resourceType].forEach(resource => {
          if (resource.templateMetaName === templateMetaName) {
            resource.shouldInstall = shouldInstall
            if (!shouldInstall) {
              templateToInstall.resourcesToSkip[resource.templateMetaName] =
                resource.kind
            } else if (
              templateToInstall.resourcesToSkip[resource.templateMetaName]
            ) {
              // if we re-check a resource that we un-checked, remove it from the skipped resources hash
              delete templateToInstall.resourcesToSkip[
                resource.templateMetaName
              ]
            }
          }
        })

        draftState.communityTemplateToInstall = templateToInstall
        return
      }

      case SET_STACKS: {
        const {stacks} = action

        draftState.stacks = stacks
        return
      }
    }
  })

export default templatesReducer
