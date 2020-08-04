/**
 * PLEASE READ
 * This files has been created as a way to effectively test
 * the getTimeRangeWithTimezone function since current system (circleCI, Jenkins)
 * and JS Date limitations prevent us from fully testing out its dependent functions
 *
 * It should be noted that the native getTimezoneOffset function returns a number
 * that represents the number of minutes (not hours) the "local" timezone is offset
 * where locations West of UTC are positive (+420) and locations East of UTC are negative (-120):
 *
 * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/getTimezoneOffset
 **/

export const getTimezoneOffset = (): number => new Date().getTimezoneOffset()
