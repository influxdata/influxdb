import * as Types from 'src/types/modules'

export const errorThrown = (
  error: Types.Errors.Data.ErrorDescription,
  altText?: string,
  alertType?: Types.Errors.Data.AlertType
): Types.Errors.Actions.ErrorThrownAction => ({
  type: 'ERROR_THROWN',
  error,
  altText,
  alertType,
})
