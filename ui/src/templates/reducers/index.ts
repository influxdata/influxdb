import {produce} from 'immer'
import {
  Action,
  ADD_TEMPLATE_SUMMARY,
  POPULATE_TEMPLATE_SUMMARIES,
  REMOVE_TEMPLATE_SUMMARY,
  SET_ACTIVE_COMMUNITY_TEMPLATE,
  SET_EXPORT_TEMPLATE,
  SET_TEMPLATE_SUMMARY,
  SET_TEMPLATES_STATUS,
  TOGGLE_TEMPLATE_RESOURCE_INSTALL,
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

export const defaultState = (): TemplatesState => ({
  activeCommunityTemplate: {} as CommunityTemplate,
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  exportTemplate: {
    status: RemoteDataState.NotStarted,
    item: null,
  },
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

      case SET_ACTIVE_COMMUNITY_TEMPLATE: {
        const {template} = action

        const activeCommunityTemplate: CommunityTemplate = {}

        activeCommunityTemplate.dashboards = (template.dashboards || []).map(
          dashboard => {
            if (!dashboard.hasOwnProperty('shouldInstall')) {
              dashboard.shouldInstall = true
            }
            return dashboard
          }
        )

        activeCommunityTemplate.telegrafConfigs = (
          template.telegrafConfigs || []
        ).map(telegrafConfig => {
          if (!telegrafConfig.hasOwnProperty('shouldInstall')) {
            telegrafConfig.shouldInstall = true
          }
          return telegrafConfig
        })

        activeCommunityTemplate.buckets = (template.buckets || []).map(
          bucket => {
            if (!bucket.hasOwnProperty('shouldInstall')) {
              bucket.shouldInstall = true
            }
            return bucket
          }
        )

        activeCommunityTemplate.checks = (template.checks || []).map(check => {
          if (!check.hasOwnProperty('shouldInstall')) {
            check.shouldInstall = true
          }
          return check
        })

        activeCommunityTemplate.variables = (template.variables || []).map(
          variable => {
            if (!variable.hasOwnProperty('shouldInstall')) {
              variable.shouldInstall = true
            }
            return variable
          }
        )

        activeCommunityTemplate.notificationRules = (
          template.notificationRules || []
        ).map(noritifcationRule => {
          if (!noritifcationRule.hasOwnProperty('shouldInstall')) {
            noritifcationRule.shouldInstall = true
          }
          return noritifcationRule
        })

        activeCommunityTemplate.labels = (template.labels || []).map(label => {
          if (!label.hasOwnProperty('shouldInstall')) {
            label.shouldInstall = true
          }
          return label
        })

        draftState.activeCommunityTemplate = activeCommunityTemplate
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

        const activeCommTempl = {...draftState.activeCommunityTemplate}

        activeCommTempl[resourceType].forEach(resource => {
          if (resource.templateMetaName === templateMetaName) {
            resource.shouldInstall = shouldInstall
          }
        })

        draftState.activeCommunityTemplate = activeCommTempl
      }
    }
  })

export default templatesReducer
